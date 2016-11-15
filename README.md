Elasticsearch checker for nagios and icinga
===================

----------


Dependencies
-------------
* pip install PySock urllib3 elasticsearch 

----------


Functionalities
-------------------

* Execute a elasticsearch query and parse result
* Check status cluster health


Examples
-------------

* python check_es.py --host 127.0.0.1 --port 9200 --index winlogbeat-* --fields-to-be-returned message,computer_name,source_name --critical 1 --query $QUERY
