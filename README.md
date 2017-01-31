# htcondor_descript

Small tool to automatically generate the description file for the
`condor_submit` command.

I thought this would be useful for some users of
[HTCondor](https://research.cs.wisc.edu/htcondor/) (job scheduler for a
computer cluster) who don't want to open a text file every time they need to
run a job. In addition this will help automate the creation of description file
for condor submission, for example with pipeline managers like snakemake.


