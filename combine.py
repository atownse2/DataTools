import os
import textwrap

combine_release_dir = "/project01/ndcms/atownse2/Combine/CMSSW_14_1_0_pre4"

def run_in_cmssw(workspace_dir, command):
    command = textwrap.dedent(f"""
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        cd {combine_release_dir}/src
        eval `scramv1 runtime -sh`
        cd {workspace_dir}
        {command}
        """)
    os.system(command)

def run_combine(datacard, method, workspace_dir, extra_args=""):

    if extra_args != "":
        command = f"combine -M {method} -d {datacard} {extra_args}\n"
    else:
        command = f"combine -M {method} {datacard}\n"
    run_in_cmssw(workspace_dir, command)

def read_asymptotic_limit(root_file):
    import ROOT
    if not os.path.exists(root_file):
        print(f"Error: {root_file} does not exist.")
        return None

    limit_keys = ['Expected  2.5%', 'Expected 16.0%', 'Expected 50.0%',
                  'Expected 84.0%', 'Expected 97.5%', 'Observed Limit']
    limit = {}

    asymptotic = ROOT.TFile(root_file)
    if not asymptotic or asymptotic.IsZombie():
        print(f"Error: {root_file} is not a valid file.")
        return None
    trees = asymptotic.Get("limit")
    if trees is None or not isinstance(trees, ROOT.TTree):
        print(f"Error: {root_file} does not contain a valid tree.")
        return None
    if trees.GetEntries() != len(limit_keys):
        print(f"Error: {root_file} does not contain the expected number of trees.")
        return None
    for tree, key in zip(trees, limit_keys):
        limit[key] = tree.limit
    asymptotic.Close()
    return limit

def read_hybrid_limit(root_file):
    import ROOT
    if not os.path.exists(root_file):
        print(f"Error: {root_file} does not exist.")
        return None

    hybrid = ROOT.TFile(root_file)
    if not hybrid or hybrid.IsZombie():
        print(f"Error: {root_file} is not a valid file.")
        return None
    tree = hybrid.Get("limit")
    if tree is None or not isinstance(tree, ROOT.TTree):
        print(f"Error: {root_file} does not contain a valid tree.")
        return None
    tree.GetEntry(0)
    return {"limit": tree.limit, "limitErr": tree.limitErr, "quantileExpected": tree.quantileExpected}

def read_asymptotic_significance(root_file):
    import ROOT
    if not os.path.exists(root_file):
        print(f"Error: {root_file} does not exist.")
        return None

    significance = ROOT.TFile(root_file)
    if not significance or significance.IsZombie():
        print(f"Error: {root_file} is not a valid file.")
        return None
    tree = significance.Get("limit")
    if tree is None or not isinstance(tree, ROOT.TTree):
        print(f"Error: {root_file} does not contain a valid tree.")
        return None
    tree.GetEntry(0)
    return {"significance_asymptotic": tree.limit, "significanceErr_asymptotic": tree.limitErr}

def read_hybrid_significance(root_file):
    import ROOT
    if not os.path.exists(root_file):
        print(f"Error: {root_file} does not exist.")
        return None

    significance = ROOT.TFile(root_file)
    if not significance or significance.IsZombie():
        print(f"Error: {root_file} is not a valid file.")
        return None
    tree = significance.Get("limit")
    if tree is None or not isinstance(tree, ROOT.TTree):
        print(f"Error: {root_file} does not contain a valid tree.")
        return None
    tree.GetEntry(0)
    return {"significance_toys": tree.limit, "significanceErr_toys": tree.limitErr}