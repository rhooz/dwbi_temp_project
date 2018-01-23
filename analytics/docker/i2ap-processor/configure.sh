#!/usr/bin/env bash
# assess some arguments
CALLER=none
for i in "$@"
do
case $i in
  -c=*|--caller=*)
  CALLER="${i#*=}"
  shift
  ;;
esac
done

# if the container was run with interactive==true then spawn a shell
if [ "$INTERACTIVE" = "true" ] && [ "$CALLER" = "none" ]; then
    echo "opening a terminal for interactive use"
    /bin/bash
fi
