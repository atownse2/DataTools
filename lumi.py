#!/usr/bin/env python
# imported from https://github.com/CERN-PH-CMG/cmg-cmssw/blob/0c11a5a0a15c4c3e1a648c9707b06b08b747b0c0/PhysicsTools/Heppy/scripts/heppy_report.py
from optparse import OptionParser
import json
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True

def root2map(tree):
    tree.SetBranchStatus("*", 0)
    tree.SetBranchStatus("run", 1)
    tree.SetBranchStatus("luminosityBlock", 1)
    jsonind = {}
    for e in range(tree.GetEntries()):
        tree.GetEntry(e)
        run, lumi = tree.run, tree.luminosityBlock
        if run not in jsonind:
            jsonind[run] = [lumi]
        else:
            jsonind[run].append(lumi)
    # remove duplicates
    for run in jsonind:
        jsonind[run] = list(set(jsonind[run]))

    nruns = len(jsonind)
    nlumis = sum(len(v) for v in jsonind.values())
    jsonmap = {}
    for r, lumis in jsonind.items():
        if len(lumis) == 0:
            continue  # shouldn't happen
        lumis.sort()
        ranges = [[lumis[0], lumis[0]]]
        for lumi in lumis[1:]:
            if lumi == ranges[-1][1] + 1:
                ranges[-1][1] = lumi
            else:
                ranges.append([lumi, lumi])
        jsonmap[r] = ranges
    return (jsonmap, nruns, nlumis)


if __name__ == '__main__':
    import os

    from data_tools.storage_config import cache_dir

    lumi_cache_dir = cache_dir+'/lumi'
    if not os.path.exists(lumi_cache_dir):
        os.makedirs(lumi_cache_dir)

    data_dir = "/project01/ndcms/atownse2/data/MLNanoAODv9"
    for ERA in ['2016_HIPM', '2016', '2017', '2018']:
        output_file = f"{lumi_cache_dir}/lumi_summary_{ERA}.json"
        if os.path.exists(output_file):
            print(f"Lumi summary for {ERA} already exists")
            continue

        files = []
        for file in os.listdir(data_dir):
            if 'HIPM' in ERA:
                year = ERA.split('_')[0]
                if f"Run{year}" in file and 'HIPM' in file:
                    files.append(f'{data_dir}/{file}')
            else:
                if f"Run{ERA}" in file:
                    files.append(f'{data_dir}/{file}')

        chain = ROOT.TChain("LuminosityBlocks")
        for f in files:
            chain.Add(f)
        summary = root2map(chain)
        if summary:
            jmap, runs, lumis = summary
            json.dump(jmap, open(output_file, 'w'), indent=4)
            print("Saved %s (%d runs, %d lumis)" % (output_file, runs, lumis))