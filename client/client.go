package client

import (
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	"github.com/Sirupsen/logrus"
	tuf "github.com/endophage/gotuf"
	"github.com/endophage/gotuf/data"
	"github.com/endophage/gotuf/keys"
	"github.com/endophage/gotuf/signed"
	"github.com/endophage/gotuf/store"
	"github.com/endophage/gotuf/utils"
)

type Client struct {
	local  *tuf.TufRepo
	remote store.RemoteStore
	keysDB *keys.KeyDB
	cache  *store.FileCacheStore
}

func NewClient(local *tuf.TufRepo, remote store.RemoteStore, keysDB *keys.KeyDB, cachePath string) *Client {
	cache := store.NewFileCacheStore(cachePath)
	return &Client{
		local:  local,
		remote: remote,
		keysDB: keysDB,
		cache:  cache,
	}
}

func (c *Client) Update() error {
	// 1. Get timestamp
	//   a. If timestamp error (verification, expired, etc...) download new root and return to 1.
	// 2. Check if local snapshot is up to date
	//   a. If out of data, get updated snapshot
	//     i. If snapshot error, download new root and return to 1.
	// 3. Check if root correct against snapshot
	//   a. If incorrect, download new root and return to 1.
	// 4. Iteratively download and search targets and delegations to find target meta
	err := c.update()
	if err != nil {
		switch err.(type) {
		case tuf.ErrSigVerifyFail:
		case tuf.ErrMetaExpired:
		case tuf.ErrLocalRootExpired:
			if err := c.downloadRoot(); err != nil {
				logrus.Errorf("Client Update (Root):", err)
				return err
			}
		default:
			return err
		}
	}
	// If we error again, we now have the latest root and just want to fail
	// out as there's no expectation the problem can be resolved automatically
	c.update()
	if err != nil {
		return err
	}
}

func (c *Client) update() error {
	err := c.downloadTimestamp()
	if err != nil {
		logrus.Errorf("Client Update (Timestamp): ", err.Error())
		return err
	}
	err = c.downloadSnapshot()
	if err != nil {
		logrus.Errorf("Client Update (Snapshot): ", err.Error())
		return err
	}
	err = c.checkRoot()
	if err != nil {
		return err
	}
	// will always need top level targets at a minimum
	err = c.downloadTargets("targets")
	if err != nil {
		logrus.Errorf("Client Update (Targets): ", err.Error())
		return err
	}
	return nil
}

// checkRoot determines if the hash, and size are still those reported
// in the snapshot file. It will also check the expiry, however, if the
// hash and size in snapshot are unchanged but the root file has expired,
// there is little expectation that the situation can be remedied.
func (c Client) checkRoot() error {
	return nil
}

// downloadRoot is responsible for downloading the root.json
func (c *Client) downloadRoot() error {
	role := data.RoleName("root")
	size := c.local.Snapshot.Signed.Meta[role].Length

	raw, err := c.remote.GetMeta(role, size)
	if err != nil {
		return err
	}
	s := &data.Signed{}
	err = json.Unmarshal(raw, s)
	if err != nil {
		return err
	}
	err = signed.Verify(s, role, 0, c.keysDB)
	if err != nil {
		return err
	}
	c.local.SetRoot(s)
	return nil
}

// downloadTimestamp is responsible for downloading the timestamp.json
func (c *Client) downloadTimestamp() error {
	role := data.RoleName("timestamp")
	raw, err := c.remote.GetMeta(role, 5<<20)
	if err != nil {
		return err
	}
	s := &data.Signed{}
	err = json.Unmarshal(raw, s)
	if err != nil {
		return err
	}
	err = signed.Verify(s, role, 0, c.keysDB)
	if err != nil {
		return err
	}
	c.local.SetTimestamp(s)
	return nil
}

// downloadSnapshot is responsible for downloading the snapshot.json
func (c *Client) downloadSnapshot() error {
	role := data.RoleName("snapshot")
	size := c.local.Timestamp.Signed.Meta[role].Length
	raw, err := c.remote.GetMeta(role, size)
	if err != nil {
		return err
	}
	s := &data.Signed{}
	err = json.Unmarshal(raw, s)
	if err != nil {
		return err
	}
	err = signed.Verify(s, role, 0, c.keysDB)
	if err != nil {
		return err
	}
	c.local.SetSnapshot(s)
	return nil
}

// downloadTargets is responsible for downloading any targets file
// including delegates roles. It will download the whole tree of
// delegated roles below the given one
func (c *Client) downloadTargets(role string) error {
	role = data.RoleName(role) // this will really only do something for base targets role
	snap := c.local.Snapshot.Signed
	root := c.local.Root.Signed
	r := c.keysDB.GetRole(role)
	if r == nil {
		return fmt.Errorf("Invalid role: %s", role)
	}
	keyIDs := r.KeyIDs
	s, err := c.GetTargetsFile(role, keyIDs, snap.Meta, root.ConsistentSnapshot, r.Threshold)
	if err != nil {
		logrus.Error("Error getting targets file:", err)
		return err
	}
	err = c.local.SetTargets(role, s)
	if err != nil {
		return err
	}

	return nil
}

func (c Client) GetTargetsFile(roleName string, keyIDs []string, snapshotMeta data.Files, consistent bool, threshold int) (*data.Signed, error) {
	// require role exists in snapshots
	roleMeta, ok := snapshotMeta[roleName]
	if !ok {
		return nil, fmt.Errorf("Snapshot does not contain target role")
	}
	hashSha256, ok := hashes["sha256"]
	if !ok {
		return "", fmt.Errorf("Consistent Snapshots Enabled and sha256 not found for targets file in snapshot meta")
	}

	// try to get meta file from content addressed cache
	r, err := c.cache.Get(hashSha256, roleMeta.Length)
	if err != nil || len(r) == 0 {
		rolePath, err := c.RoleTargetsPath(roleName, hashSha256, consistent)
		if err != nil {
			return nil, err
		}
		//roleSha256, err :=
		r, err := c.remote.GetMeta(rolePath, snapshotMeta[roleName].Length)
		if err != nil {
			return nil, err
		}
		c.cache.Set(hashSha256, r)
	}
	s := &data.Signed{}
	err = json.Unmarshal(r, s)
	if err != nil {
		logrus.Error("Error unmarshalling targets file:", err)
		return nil, err
	}
	err = signed.Verify(s, roleName, 0, c.keysDB)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// RoleTargetsPath generates the appropriate filename for the targes file,
// based on whether the repo is marked as consistent.
func (c Client) RoleTargetsPath(roleName string, hashSha256 string, consistent bool) (string, error) {
	if consistent {
		dir := filepath.Dir(roleName)
		if strings.Contains(roleName, "/") {
			lastSlashIdx := strings.LastIndex(roleName, "/")
			roleName = roleName[lastSlashIdx+1:]
		}
		roleName = path.Join(
			dir,
			fmt.Sprintf("%s.%s.json", hasheSha256, roleName),
		)
	}
	return roleName, nil
}

// TargetMeta ensures the repo is up to date, downloading the minimum
// necessary metadata files
func (c Client) TargetMeta(path string) (*data.FileMeta, error) {
	c.Update()
	var meta *data.FileMeta

	// FIFO list of targets delegations to inspect for target
	roles := []string{data.ValidRoles["targets"]}
	for ; len(roles) > 0; role := roles[0] {
		roles = roles[1:]
		// Download the target role file if necessary
		err = c.downloadTargets(role)
		if err != nil {
			// as long as we find a valid target somewhere we're happy.
			// continue and search other delegated roles if any
			continue
		}

		meta = c.local.TargetMeta(role, path)
		if meta != nil {
			// we found the target!
			break
		}
		delegations := c.local.TargetDelegations(role, path, pathHex)
		for _, d := range delegations {
			roles = append(roles, d.Name)
		}
	}
	return meta, nil
}

func (c Client) DownloadTarget(dst io.Writer, path string, meta *data.FileMeta) error {
	reader, err := c.remote.GetTarget(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	r := io.TeeReader(
		io.LimitReader(reader, meta.Length),
		dst,
	)
	err = utils.ValidateTarget(r, meta)
	return err
}
