package dynamolocker

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/tus/tusd"
)

func TestDyanmoLocker(t *testing.T) {
	a := assert.New(t)

	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	})
	if err != nil {
		t.Fatalf("failed to connect to local dynamoDB: %v", err)
	}
	dbSvc := dynamodb.New(sess)
	tableName := "test-locker"
	locker, err := New(dbSvc, "test-locker")
	a.NoError(err)
	locker.Client.CreateTable(tableName,
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	)

	locker2, err := New(dbSvc, "test-locker")
	a.NoError(err)

	a.NoError(locker.LockUpload("one"))
	a.Equal(tusd.ErrFileLocked, locker.LockUpload("one"))
	time.Sleep(3 * time.Second)
	// test that lock remains between heartbeats
	a.Equal(tusd.ErrFileLocked, locker.LockUpload("one"))
	// test that the lock cannot be taken by a second client
	a.Equal(tusd.ErrFileLocked, locker2.LockUpload("one"))
	a.NoError(locker.UnlockUpload("one"))
	a.Equal(ErrLockNotHeld, locker.UnlockUpload("one"))
	a.NoError(locker2.LockUpload("one"))
	a.Equal(tusd.ErrFileLocked, locker2.LockUpload("one"))
	a.NoError(locker2.UnlockUpload("one"))
	a.Equal(ErrLockNotHeld, locker2.UnlockUpload("one"))
}
