package os

import (
	"fmt"
	"io/fs"
	goos "os"
	"strconv"
	"sync/atomic"
	"time"

	tw "github.com/olekukonko/tablewriter"
)

type FileMode = fs.FileMode
type FileInfo = fs.FileInfo
type LinkError = goos.LinkError

const (
	frequency = 5

	O_APPEND      = goos.O_APPEND
	O_CREATE      = goos.O_CREATE
	O_EXCL        = goos.O_EXCL
	O_RDONLY      = goos.O_RDONLY
	O_RDWR        = goos.O_RDWR
	O_WRONLY      = goos.O_WRONLY
	ModeExclusive = goos.ModeExclusive
	ModeSymlink   = goos.ModeSymlink
)

var (
	cntChtimes     = atomic.Int32{}
	cntCreate      = atomic.Int32{}
	cntLstat       = atomic.Int32{}
	cntMkdirAll    = atomic.Int32{}
	cntOpen        = atomic.Int32{}
	cntOpenFile    = atomic.Int32{}
	cntReadFile    = atomic.Int32{}
	cntReadlink    = atomic.Int32{}
	cntRename      = atomic.Int32{}
	cntRemove      = atomic.Int32{}
	cntRemoveAll   = atomic.Int32{}
	cntStat        = atomic.Int32{}
	cntSymlink     = atomic.Int32{}
	cntWriteFile   = atomic.Int32{}
	totalChtimes   = atomic.Int32{}
	totalCreate    = atomic.Int32{}
	totalLstat     = atomic.Int32{}
	totalMkdirAll  = atomic.Int32{}
	totalOpen      = atomic.Int32{}
	totalOpenFile  = atomic.Int32{}
	totalReadFile  = atomic.Int32{}
	totalReadlink  = atomic.Int32{}
	totalRename    = atomic.Int32{}
	totalRemove    = atomic.Int32{}
	totalRemoveAll = atomic.Int32{}
	totalStat      = atomic.Int32{}
	totalSymlink   = atomic.Int32{}
	totalWriteFile = atomic.Int32{}

	ErrClosed = goos.ErrClosed
	Stderr    = goos.Stderr
)

func init() {
	go func() {
		for {

			table := tw.NewWriter(goos.Stdout)
			table.SetHeader([]string{"Function", fmt.Sprintf("last %d second(s)", frequency), "total"})
			table.SetAutoFormatHeaders(false)
			table.Append([]string{"Chtimes", strconv.FormatInt(int64(cntChtimes.Load()), 10), strconv.FormatInt(int64(totalChtimes.Load()), 10)})
			table.Append([]string{"Create", strconv.FormatInt(int64(cntCreate.Load()), 10), strconv.FormatInt(int64(totalCreate.Load()), 10)})
			table.Append([]string{"Open", strconv.FormatInt(int64(cntOpen.Load()), 10), strconv.FormatInt(int64(totalOpen.Load()), 10)})
			table.Append([]string{"OpenFile", strconv.FormatInt(int64(cntOpenFile.Load()), 10), strconv.FormatInt(int64(totalOpenFile.Load()), 10)})
			table.Append([]string{"Lstat", strconv.FormatInt(int64(cntLstat.Load()), 10), strconv.FormatInt(int64(totalLstat.Load()), 10)})
			table.Append([]string{"MkdirAll", strconv.FormatInt(int64(cntMkdirAll.Load()), 10), strconv.FormatInt(int64(totalMkdirAll.Load()), 10)})
			table.Append([]string{"ReadFile", strconv.FormatInt(int64(cntReadFile.Load()), 10), strconv.FormatInt(int64(totalReadFile.Load()), 10)})
			table.Append([]string{"Readlink", strconv.FormatInt(int64(cntReadlink.Load()), 10), strconv.FormatInt(int64(totalReadlink.Load()), 10)})
			table.Append([]string{"Rename", strconv.FormatInt(int64(cntRename.Load()), 10), strconv.FormatInt(int64(totalRename.Load()), 10)})
			table.Append([]string{"Remove", strconv.FormatInt(int64(cntRemove.Load()), 10), strconv.FormatInt(int64(totalRemove.Load()), 10)})
			table.Append([]string{"RemoveAll", strconv.FormatInt(int64(cntRemoveAll.Load()), 10), strconv.FormatInt(int64(totalRemoveAll.Load()), 10)})
			table.Append([]string{"Stat", strconv.FormatInt(int64(cntStat.Load()), 10), strconv.FormatInt(int64(totalStat.Load()), 10)})
			table.Append([]string{"Symlink", strconv.FormatInt(int64(cntSymlink.Load()), 10), strconv.FormatInt(int64(totalSymlink.Load()), 10)})
			table.Append([]string{"WriteFile", strconv.FormatInt(int64(cntWriteFile.Load()), 10), strconv.FormatInt(int64(totalWriteFile.Load()), 10)})
			table.Render()

			cntChtimes.Store(0)
			cntCreate.Store(0)
			cntLstat.Store(0)
			cntMkdirAll.Store(0)
			cntOpen.Store(0)
			cntOpenFile.Store(0)
			cntReadFile.Store(0)
			cntReadlink.Store(0)
			cntRename.Store(0)
			cntRemove.Store(0)
			cntRemoveAll.Store(0)
			cntStat.Store(0)
			cntSymlink.Store(0)
			cntWriteFile.Store(0)

			time.Sleep(frequency * time.Second)
		}
	}()
}

func Chtimes(path string, atime, mtime time.Time) error {
	cntChtimes.Add(1)
	totalChtimes.Add(1)
	return goos.Chtimes(path, atime, mtime)
}

func Create(path string) (*goos.File, error) {
	cntCreate.Add(1)
	totalCreate.Add(1)
	return goos.Create(path)
}

func Lstat(path string) (fs.FileInfo, error) {
	cntLstat.Add(1)
	totalLstat.Add(1)
	return goos.Lstat(path)
}

func MkdirAll(path string, perm goos.FileMode) error {
	cntMkdirAll.Add(1)
	totalMkdirAll.Add(1)
	return goos.MkdirAll(path, perm)
}

func Open(name string) (*goos.File, error) {
	cntOpen.Add(1)
	totalOpen.Add(1)
	return goos.Open(name)
}

func OpenFile(name string, flag int, perm goos.FileMode) (*goos.File, error) {
	cntOpen.Add(1)
	totalOpen.Add(1)
	return goos.OpenFile(name, flag, perm)
}

func ReadFile(path string) ([]byte, error) {
	cntReadFile.Add(1)
	totalReadFile.Add(1)
	return goos.ReadFile(path)
}

func Readlink(path string) (string, error) {
	cntReadlink.Add(1)
	totalReadlink.Add(1)
	return goos.Readlink(path)
}

func Rename(oldname, newname string) error {
	cntRename.Add(1)
	totalRename.Add(1)
	return goos.Rename(oldname, newname)
}

func RemoveAll(path string) error {
	cntRemoveAll.Add(1)
	totalRemoveAll.Add(1)
	return goos.RemoveAll(path)
}

func Remove(path string) error {
	cntRemove.Add(1)
	totalRemove.Add(1)
	return goos.Remove(path)
}

func Stat(path string) (fs.FileInfo, error) {
	cntStat.Add(1)
	totalStat.Add(1)
	return goos.Stat(path)
}

func Symlink(oldname, newname string) error {
	cntSymlink.Add(1)
	totalSymlink.Add(1)
	return goos.Symlink(oldname, newname)
}

func WriteFile(oldname string, data []byte, perm goos.FileMode) error {
	cntSymlink.Add(1)
	totalSymlink.Add(1)
	return goos.WriteFile(oldname, data, perm)
}

func IsNotExist(err error) bool {
	return goos.IsNotExist(err)
}

func Getpid() int {
	return goos.Getpid()
}
