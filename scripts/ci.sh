#!/bin/bash

set -e
set -x

step="$1"

echo "Running step $step ..."

case "$step" in
    leveldb)
        wget https://github.com/google/leveldb/archive/v1.19.tar.gz
        tar -xzf v1.19.tar.gz
        cd leveldb-1.19
        make
        sudo mv libleveldb.* /usr/local/lib
        cd include
        sudo cp -R leveldb /usr/local/include
        sudo ldconfig
        ;;
    install)
        mkdir -p $HOME/.local/bin
        case "$BUILD" in
            stack)
                stack $STACK_ARGS setup --no-terminal
                stack $STACK_ARGS build --fast --only-snapshot --no-terminal
                ;;
            cabal)
                sed -i 's/^jobs:/-- jobs:/' ${HOME}/.cabal/config;
                cabal new-build --enable-tests -j4 --dep powerqueue powerqueue-levelmem
                ;;
        esac
        ;;
    script)
        case "$BUILD" in
            stack)
                stack $STACK_ARGS $STACK_BUILD_MODE --pedantic --fast --no-terminal --skip-ghc-check $STACK_BUILD_ARGS
                ;;
            cabal)
                cabal new-build --enable-tests -j4 --dep powerqueue powerqueue-levelmem
                ;;
        esac
        ;;
    *)
        echo "Bad step: $step"
        exit 1
        ;;
esac

echo "Completed $step ."
exit 0
