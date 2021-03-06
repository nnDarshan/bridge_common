Name: tendrl-commons
Version: 1.2
Release: 2%{?dist}
BuildArch: noarch
Summary: Common lib for Tendrl sds integrations and node-agent
Source0: %{name}-%{version}.tar.gz
License: LGPLv2+
URL: https://github.com/Tendrl/commons

BuildRequires: ansible >= 2.2
BuildRequires: pytest
BuildRequires: python2-devel
BuildRequires: python-mock
BuildRequires: python-six
BuildRequires: systemd
BuildRequires: python-yaml

Requires: ansible >= 2.2
Requires: namespaces
Requires: python-dateutil
Requires: python-dns
Requires: python-etcd
Requires: systemd-python
Requires: python-urllib3
Requires: python-six
Requires: python-docutils
Requires: python-yaml

%description
Common lib for Tendrl sds integrations and node-agent

%prep
%setup

# Remove bundled egg-info
rm -rf %{name}.egg-info

%build
%{__python} setup.py build

%install
%{__python} setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%check
py.test -v tendrl/commons/tests || :

%files -f INSTALLED_FILES
%doc README.rst
%license LICENSE

%changelog
* Tue Dec 06 2016 Martin Bukatovič <mbukatov@redhat.com> - 0.0.1-2
- Fixed https://github.com/Tendrl/commons/issues/72

* Mon Oct 17 2016 Timothy Asir Jeyasingh <tjeyasin@redhat.com> - 0.0.1-1
- Initial build.
