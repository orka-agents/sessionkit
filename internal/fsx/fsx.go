// Package fsx performs descriptor-relative filesystem operations without following
// symlinks. It supports the Linux and macOS hosts supported by Codex.
package fsx

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type Root struct{ dir *os.File }

func ValidPath(name string) bool {
	return name != "" && name != "." && fs.ValidPath(name) && !strings.ContainsAny(name, "\\\x00")
}

// OpenRoot rejects a symlink at every path component, including the root itself.
func OpenRoot(name string) (*Root, error) {
	if !filepath.IsAbs(name) {
		return nil, fmt.Errorf("root must be absolute")
	}
	f, err := os.Open("/")
	if err != nil {
		return nil, err
	}
	r := &Root{dir: f}
	rel := strings.TrimPrefix(filepath.Clean(name), "/")
	if rel == "" {
		return r, nil
	}
	d, err := r.openDir(rel, false)
	_ = f.Close()
	if err != nil {
		return nil, err
	}
	return &Root{dir: d}, nil
}

func (r *Root) Close() error { return r.dir.Close() }

// Sub opens a directory through the retained root without reopening its path.
func (r *Root) Sub(name string) (*Root, error) {
	dir, err := r.openDir(name, false)
	if err != nil {
		return nil, err
	}
	return &Root{dir: dir}, nil
}

// CheckPath detects a root renamed or replaced while descriptors remain open.
func (r *Root) CheckPath(name string) error {
	current, err := OpenRoot(name)
	if err != nil {
		return err
	}
	defer func() { _ = current.Close() }()
	expected, err := r.dir.Stat()
	if err != nil {
		return err
	}
	actual, err := current.dir.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(expected, actual) {
		return fmt.Errorf("root directory was moved or replaced")
	}
	return nil
}

// Contains checks directory ancestry by identity, including case aliases.
func (r *Root) Contains(other *Root) (bool, error) {
	expected, err := r.dir.Stat()
	if err != nil {
		return false, err
	}
	current, err := other.openDir(".", false)
	if err != nil {
		return false, err
	}
	defer func() { _ = current.Close() }()
	for {
		actual, err := current.Stat()
		if err != nil {
			return false, err
		}
		if os.SameFile(expected, actual) {
			return true, nil
		}
		fd, err := unix.Openat(int(current.Fd()), "..", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return false, err
		}
		parent := os.NewFile(uintptr(fd), "..")
		parentInfo, err := parent.Stat()
		_ = current.Close()
		current = parent
		if err != nil {
			return false, err
		}
		if os.SameFile(actual, parentInfo) {
			return false, nil
		}
	}
}

func (r *Root) openDir(name string, create bool) (*os.File, error) {
	if name != "." && !ValidPath(name) {
		return nil, fmt.Errorf("invalid relative directory")
	}
	fd, err := unix.Openat(int(r.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	if name == "." {
		return os.NewFile(uintptr(fd), name), nil
	}
	for _, part := range strings.Split(name, "/") {
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if create && errors.Is(e, unix.ENOENT) {
			e = unix.Mkdirat(fd, part, 0700)
			if e == nil {
				e = unix.Fsync(fd)
			}
			if e != nil && !errors.Is(e, unix.EEXIST) {
				_ = unix.Close(fd)
				return nil, e
			}
			next, e = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		_ = unix.Close(fd)
		if e != nil {
			return nil, &os.PathError{Op: "open directory", Path: name, Err: e}
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (r *Root) MkdirAll(name string) error {
	d, err := r.openDir(name, true)
	if err != nil {
		return err
	}
	return d.Close()
}

// Mkdir creates a private directory. The caller must sync its parent after
// registering cleanup for any subsequent failure.
func (r *Root) Mkdir(name string) error {
	if !ValidPath(name) {
		return fmt.Errorf("invalid directory path")
	}
	d, err := r.openDir(path.Dir(name), false)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return unix.Mkdirat(int(d.Fd()), path.Base(name), 0700)
}
func (r *Root) RemoveDir(name string) error {
	if !ValidPath(name) {
		return fmt.Errorf("invalid directory path")
	}
	if path.Dir(name) == "." {
		return unix.Unlinkat(int(r.dir.Fd()), name, unix.AT_REMOVEDIR)
	}
	d, err := r.openDir(path.Dir(name), false)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return unix.Unlinkat(int(d.Fd()), path.Base(name), unix.AT_REMOVEDIR)
}
func (r *Root) SyncDir(name string) error {
	d, err := r.openDir(name, false)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func (r *Root) Open(name string) (*os.File, error) {
	return r.open(name, unix.O_RDONLY|unix.O_NONBLOCK, 0)
}
func (r *Root) Create(name string) (*os.File, error) {
	return r.open(name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0600)
}
func (r *Root) LockFile(name string) (*os.File, error) {
	// Separate creation from reopening an existing lock inode. Concurrent
	// non-exclusive O_CREAT opens can return ENOENT on macOS.
	f, err := r.open(name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NONBLOCK, 0600)
	if errors.Is(err, os.ErrExist) {
		return r.open(name, unix.O_RDWR|unix.O_NONBLOCK, 0)
	}
	return f, err
}
func (r *Root) open(name string, flags int, mode uint32) (*os.File, error) {
	if !ValidPath(name) {
		return nil, fmt.Errorf("invalid relative file path")
	}
	d, err := r.openDir(path.Dir(name), false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = d.Close() }()
	fd, err := unix.Openat(int(d.Fd()), path.Base(name), flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, mode)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	f := os.NewFile(uintptr(fd), name)
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("component is not a regular file")
	}
	return f, nil
}

func (r *Root) Link(oldname, newname string) error {
	if !ValidPath(oldname) || !ValidPath(newname) {
		return fmt.Errorf("invalid link path")
	}
	old, err := r.openDir(path.Dir(oldname), false)
	if err != nil {
		return err
	}
	defer func() { _ = old.Close() }()
	next, err := r.openDir(path.Dir(newname), false)
	if err != nil {
		return err
	}
	defer func() { _ = next.Close() }()
	return unix.Linkat(int(old.Fd()), path.Base(oldname), int(next.Fd()), path.Base(newname), 0)
}
func (r *Root) Rename(oldname, newname string) error {
	if !ValidPath(oldname) || !ValidPath(newname) {
		return fmt.Errorf("invalid rename path")
	}
	old, err := r.openDir(path.Dir(oldname), false)
	if err != nil {
		return err
	}
	defer func() { _ = old.Close() }()
	next, err := r.openDir(path.Dir(newname), false)
	if err != nil {
		return err
	}
	defer func() { _ = next.Close() }()
	return unix.Renameat(int(old.Fd()), path.Base(oldname), int(next.Fd()), path.Base(newname))
}
func (r *Root) Remove(name string) error {
	if !ValidPath(name) {
		return fmt.Errorf("invalid removal path")
	}
	d, err := r.openDir(path.Dir(name), false)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return unix.Unlinkat(int(d.Fd()), path.Base(name), 0)
}

// WalkFiles reports regular files and rejects symlinks or special files. visit
// also runs for directories so callers can charge their operation's node budget.
func (r *Root) WalkFiles(name string, visit func(string, bool) error) error {
	d, err := r.openDir(name, false)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return walk(d, name, visit)
}
func walk(d *os.File, prefix string, visit func(string, bool) error) error {
	for {
		entries, err := d.Readdirnames(64)
		for _, entry := range entries {
			var stat unix.Stat_t
			if err := unix.Fstatat(int(d.Fd()), entry, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return err
			}
			mode := stat.Mode & unix.S_IFMT
			name := path.Join(prefix, entry)
			if err := visit(name, mode == unix.S_IFDIR); err != nil {
				return err
			}
			if mode == unix.S_IFLNK {
				return fmt.Errorf("symlink in session tree: %s", name)
			}
			if mode == unix.S_IFDIR {
				fd, e := unix.Openat(int(d.Fd()), entry, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
				if e != nil {
					return e
				}
				sub := os.NewFile(uintptr(fd), name)
				e = walk(sub, name, visit)
				_ = sub.Close()
				if e != nil {
					return e
				}
			} else if mode != unix.S_IFREG {
				return fmt.Errorf("special file in session tree: %s", name)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func RandomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (r *Root) WriteAtomic(name string, data []byte) error {
	id, err := RandomID()
	if err != nil {
		return err
	}
	tmp := path.Join(path.Dir(name), ".sessionkit-"+id)
	f, err := r.Create(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = r.Remove(tmp) }()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = r.Rename(tmp, name); err != nil {
		return err
	}
	return r.SyncDir(path.Dir(name))
}
