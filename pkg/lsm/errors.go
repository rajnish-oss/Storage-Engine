package lsm

import "github.com/rajnish-oss/LSM_storage_engine/internal/sstable"

var (
	ErrNotFound  = sstable.ErrNotFound
	ErrDeleted   = sstable.ErrDeleted
	EmptMemtable = sstable.EmptyMemtable
)
