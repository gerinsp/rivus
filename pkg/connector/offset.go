package connector

import (
	"context"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

func SaveSourceOffset(ctx context.Context, store meta.OffsetStore, jobID string, off *model.SourceOffset) error {
	if store == nil || !off.Valid() {
		return nil
	}
	return store.SaveOffset(ctx, jobID, meta.Offset{
		BinlogFile: off.BinlogFile,
		BinlogPos:  off.BinlogPos,
	})
}
