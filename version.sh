#!/bin/bash

VERSION=$(awk '/^## / { print $2 }' CHANGELOG.md | head -1)
MAJOR=${VERSION%.*}

sed -i~ "s/## unreleased.*/## ${VERSION} - $(date +'%Y-%m-%d')/i" CHANGELOG.md
sed -i~ "s/pyclowder==.*/pyclowder==${VERSION}/" README.md
sed -i~ "s/pyclowder==.*/pyclowder==${VERSION}/" description.rst
sed -i~ "s/pyclowder==.*/pyclowder==${VERSION}/" sample-extractors/wordcount/requirements.txt
sed -i~ -e "s/release = u'.*'/release = u'${VERSION}'/" -e "s/version = u'.*'/version = u'${MAJOR}'/" docs/source/conf.py
sed -i~ "s/version='.*'/version='${VERSION}'/" setup.py
