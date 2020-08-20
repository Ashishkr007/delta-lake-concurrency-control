# delta-lake-concurrency-control
Delta Lake uses optimistic concurrency control to provide transactional guarantees between writes.

optimistic concurrency control is ways to deal with concurrent transaction that making changes to table.

Suppose user A and user B is making changes on same table at same time. optimistic concurrency control guarantee that both transactions should complete successfully.

Now as we said above if there is no lock then how it’s possible.

To understand this let’s see how operations (Insert/Update/Delete/Alter) work on delta table.

Delta Lake transaction writes operate in three stages:

* Record: Record the starting table version.
* Read: Reads (if needed, in case of new record insert it’s not needed) the latest available version of the table to identify which files need to be modified (that is, rewritten).
* As files are immutable and can’t be modified. So, it read the file in memory and modify the data (in case of update) and rewrite into new file.
* Write: Stages all the changes by writing new data files.
* Validate and commit: Before committing the changes, checks whether the proposed changes conflict with any other changes that may have been concurrently committed since the snapshot that was read. If there are no conflicts, all the staged changes are committed as a new versioned snapshot, and the write operation succeeds.

### Conflict exceptions

When a transaction conflict occurs, you will observe one of the following exceptions:

1. ConcurrentAppendException
2. ConcurrentDeleteReadException
3. ConcurrentDeleteDeleteException
4. MetadataChangedException
5. ConcurrentTransactionException
6. ProtocolChangedException

[Read more](https://www.linkedin.com/pulse/delta-lake-concurrency-control-ashish-kumar/)
