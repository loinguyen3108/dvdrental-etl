#!/usr/bin/env bash

# Install package to ./packages folder
echo 'Update local packages for project...'

# add local modules
echo '... adding all modules from local utils package'
zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*
zip -ru9 packages.zip manager -x manager/__pycache__/\*
zip -ru9 packages.zip jobs -x jobs/__pycache__/\*
zip -ru9 packages.zip services -x services/__pycache__/\*

exit 0
