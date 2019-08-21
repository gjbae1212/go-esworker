#!/bin/bash
set -e -o pipefail
trap '[ "$?" -eq 0 ] || echo "Error Line:<$LINENO> Error Function:<${FUNCNAME}>"' EXIT
cd `dirname $0`
CURRENT=`pwd`

function test
{
    go test -v $(go list ./... | grep -v vendor) --count 1 -race -coverprofile=$CURRENT/coverage.txt -covermode=atomic
}

function bench
{
  # 10000 iterator
  go test -v -run=BenchmarkDispatcher_AddAction -bench=. -benchmem -benchtime 10005x
}

function codecov
{
   /bin/bash <(curl -s https://codecov.io/bash)
}


CMD=$1
shift
$CMD $*
