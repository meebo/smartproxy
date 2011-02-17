#!/bin/sh
python setup.py install --optimize=1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
install -d $RPM_BUILD_ROOT/var/run/lounge
install -d $RPM_BUILD_ROOT/var/log/lounge
CONFIGFILES="\
/var/run/lounge
/var/log/lounge
%config(noreplace) /etc/lounge/smartproxy.xml
%config(noreplace) /etc/lounge/smartproxy.tac"
echo "$CONFIGFILES" | cat INSTALLED_FILES - > INSTALLED_FILES.new
mv INSTALLED_FILES.new INSTALLED_FILES
