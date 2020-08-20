Small tools to fluidify working with [HTCondor][1] (job scheduler for a
computer cluster).

**Content**

- `condor_descript.py`: generate submission file in one commandline.
- `submitsplit.py`: split too large submission files.
- `condor_checklogs.py`: print stats on failed/succeeded jobs.

# condor_descript

Generate the description file for the `condor_submit` command.

I thought this would be useful for some users of
[HTCondor][1] who don't want to open a text file every time they need to
run a job. In addition this will help automate the creation of description file
for condor submission, for example with pipeline managers like snakemake.

# submitplit

Split too large submission files.

# condor_checklogs

Print stats on failed/succeeded jobs.


[1]: https://research.cs.wisc.edu/htcondor/
