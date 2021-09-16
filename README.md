# CDC_PostgreSQL

## Description

Change Data Capture (CDC) PostgreSQL is the process of recognising when data has been changed in a source system so a downstream process or system can action that change. A common use case is to reflect the change in a different target system so that the data in the systems stay in sync.

## Using

Program arguments: `-i ip` `-p port` `-u user_name` `-w password` `-d database_name` `-l publication_name` `[-r replica_slot_name]` `[-f file_path]`
