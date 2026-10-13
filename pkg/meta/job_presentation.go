package meta

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

type JobNameStore interface {
	RenameJob(context.Context, string, string) (bool, error)
}

func ValidateJobName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 255 {
		return "", fmt.Errorf("job name must contain 1 to 255 characters")
	}
	return name, nil
}

// Name is presentation metadata. It never changes submission, lease, config
// used by a running connector, or checkpoint identity.
func (s *MySQLJobStore) RenameJob(ctx context.Context, id, name string) (bool, error) {
	name, err := ValidateJobName(name)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE job_registry SET job_name=?,config_json=JSON_SET(config_json,'$.name',?),updated_at=NOW() WHERE job_id=?`, name, name, id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n > 0, err
}
