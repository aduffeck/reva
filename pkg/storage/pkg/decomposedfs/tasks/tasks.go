package tasks

import (
	"context"

	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/metadata/prefixes"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/node"
	"github.com/opencloud-eu/reva/v2/pkg/storagespace"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue"
	"github.com/opencloud-eu/reva/v2/pkg/workqueue/task"
	"github.com/rs/zerolog"
)

type DecomposedfsWorker struct {
	lu node.PathLookup

	log *zerolog.Logger
}

func Spawn(lu node.PathLookup, wq *workqueue.WorkQueue, log *zerolog.Logger) {
	dfsWorker := &DecomposedfsWorker{
		lu:  lu,
		log: log,
	}

	worker := wq.NewWorker()
	worker.Handle("node.calculate-checksums", dfsWorker.handleCalculateChecksums)
	go worker.Start()
}

func (d *DecomposedfsWorker) handleCalculateChecksums(t *task.Task) error {
	ctx := context.Background()
	id, err := storagespace.ParseID(t.Payload)
	if err != nil {
		d.log.Error().Err(err).Msg("failed to parse resource ID")
		return err
	}

	n, err := d.lu.NodeFromID(ctx, &id)
	if err != nil {
		d.log.Error().Err(err).Str("resourceID", t.Payload).Msg("failed to get node from ID")
		return err
	}
	if !n.Exists {
		d.log.Error().Str("resourceID", t.Payload).Msg("node does not exist")
		return nil
	}
	sha1h, md5h, adler32h, err := node.CalculateChecksums(ctx, n.InternalPath())
	if err != nil {
		return err
	}

	// update checksums
	attrs := node.Attributes{
		prefixes.ChecksumPrefix + "sha1":    sha1h.Sum(nil),
		prefixes.ChecksumPrefix + "md5":     md5h.Sum(nil),
		prefixes.ChecksumPrefix + "adler32": adler32h.Sum(nil),
	}

	if err := n.SetXattrs(attrs, true); err != nil {
		d.log.Error().Err(err).Str("resourceID", t.Payload).Msg("failed to set checksum attributes")
		return err
	}

	return nil
}
