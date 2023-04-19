Background
**********

Beetl was born out of an endless list of tickets for "well, if we can't have access to the data, can we have a copy on [insert-server-here]?", 
for which a lot of manual work was previously done. We've previously been using a long list of different tools for this, everything from
SQL Server replication and small scripts to export/import databases every night (which is a pain to maintain given that the database loses all knowledge of users on import),
small scripts to syncronize Azure or AD users to a database, some data from a database has to go to an XML file and be sent here and there.

This all led to the idea behind this software; a simple, easy to use, low/no-code (even though I don't love the term, that's more or less what it's supposed to be) way for
even our slightly less technical system administrators and colleagues to be able to set up the skeleton of a sync (with supervised pushes to production).

Out of that idea, BeETL was born.

Starting off, our list of specifications was as follows:

- Written in Python, Go or Rust for efficiency (We'll get back to that efficiency statement about Python later....)
- For simpler syncs, no code should be required and should in the long run work with a simple configuration interface
- A workflow that enables either basic transformation of data via included tools, or more advanced transformation through code
- Easy extensibility for new datasources and transformers
- Logging and observability - we want to be able to see what's going on and where things are going wrong
- Last, but not least, we don't want to re-create entire databases or "just update all changed rows so it's always the latest data" - we wanted a program that would only update the changed rows in the respective data source so that we'd have a better transaction log of what actually changed between runs.

The first version of BeETL was written in Go, but we quickly found that although we could have reached a similar level of performance as with Python,
the amount of code was quite a bit larger than we'd like and we currently only have one proficient Go developer. Rust hasn't been adopted at our workplace yet,
so we put that aside for the moment and had a look at Python and Pandas.

We'd worked with Pandas before, and it was usually quite quick and performant but once dealing with datasets running in the millions of rows, it started to
become somewhat of a bottleneck. After some searching, we fell upon Polars, a Python library loosely based on Pandas but with a focus on performance.

The performant parts of Polars are written in Rust, meaning we'd get all of (well, most of) the performance benefits with none of the low-level code, hooray!

After adopting one of our earlier pieces of code, the comparator (based on Pandas) to the polars equivalent, we found the performance increase compared to Pandas was massive.
On comparisons that Pandas took 30 seconds to one minute, Polars completed in just under a second; we couldn't have been happier about that.

When that was done, it was just a matter of writing the rest of the code around it, and we were done!

That was 6 months ago, and since then we've been using a (very duct-taped together version of) BeETL since, and we're finally getting to actually cleaning up the code,
documenting the software and making it open source.