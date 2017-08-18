#!/bin/bash

cd /mnt/evergreen/utilityscripts/auditor_clean
/usr/bin/du -sh /var/lib/postgresql > results.log
/usr/bin/psql -d evergreen < /mnt/evergreen/utilityscripts/auditor_clean/scrub_auditor_tables.sql >> results.log
/usr/bin/psql -d evergreen -c "SELECT \$\$VACUUM FULL ANALYZE auditor.\$\$ || table_name ||\$\$;\$\$  FROM information_schema.tables  WHERE table_schema = \$\$auditor\$\$ and table_type=\$\$BASE TABLE\$\$" | /usr/bin/perl -npe 's/^\s+//;' | /bin/sed '1,2d' | /bin/sed '$ d' > vacuum_auditor.sql
/usr/bin/psql -d evergreen < /mnt/evergreen/utilityscripts/auditor_clean/vacuum_auditor.sql >> results.log
/usr/bin/du -sh /var/lib/postgresql >> results.log