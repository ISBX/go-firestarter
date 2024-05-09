package mockfs

import (
	"testing"

	assert "github.com/stretchr/testify/assert"
)

func TestNewServer(t *testing.T) {
	assert := assert.New(t)

	server, err := newServer()
	assert.NotNil(server)
	assert.Nil(err)
}

func TestReset(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.Nil(err)

	srv.LoadFromJSONFile("test.json")
	srv.Reset()
	assert.Empty(srv.data)
}

func TestLoadJSONFile(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.Nil(err)

	err = srv.LoadFromJSONFile("test.json")
	assert.Nil(err)
	assert.NotEmpty(srv.data)

	assert.Equal("value-1-1-1", srv.data["collection-1"].documents["document-1-1"].fields["field1"])
}
