#!/bin/bash
set -eu

DIR=$(dirname $(realpath $0))

countess_cmd $DIR/simple.ini

diff $DIR/output.csv $DIR/output.csv.expected
