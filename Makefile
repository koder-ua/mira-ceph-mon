.PHONY : all clean proto lint
PROG = bin/monitor
GPATH = /home/koder/go
PROTO_FILES = $(wildcard *.proto)
GO_PB_FILES = $(PROTO_FILES:.proto=.pb.go)
PY_PB_FILES = $(PROTO_FILES:.proto=_pb2.py)
GOLINT = /home/koder/go/bin/golint

ALL_GO_FILES = $(wildcard *.go)
LINTABLE_GO_FILES = $(filter-out $(GO_PB_FILES),$(ALL_GO_FILES))

all: $(PROG) $(PY_PB_FILES) $(GO_PB_FILES)

$(PROG): *.go $(GO_PB_FILES) Makefile
	env GOPATH=$(GPATH) go build -o $(PROG)

proto: $(GO_PB_FILES) $(PY_PB_FILES)

lint:
	$(GOLINT) $(LINTABLE_GO_FILES)

%.pb.go: %.proto Makefile
	protoc --go_out=plugins=grpc:. $<

%_pb2.py: %.proto Makefile
#	protoc --python_out=plugins=grpc:. $<
	python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. $<

clean:
	rm -f $(PROG) $(GO_PB_FILES) $(PY_PB_FILES)
