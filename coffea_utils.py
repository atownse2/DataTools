import os
import json

from . dataset_info import Dataset, Datasets
from . import storage_config as stor

cache_dir = stor.ensure_cache("preprocess")
vast_cache = stor.ensure_cache("preprocess/vast")
ceph_cache = stor.ensure_cache("preprocess/ceph")

def file_is_good(file):
    from coffea.nanoevents import NanoEventsFactory
    from analysis.tools.coffea_utils import MLNanoAODSchema
    import awkward as ak
    print(f"Checking file {file}")
    events = NanoEventsFactory.from_root(
        {file: "Events"},
        schemaclass=MLNanoAODSchema,
        # delayed=False,
    ).events()
    events = events.compute()

    radions = events.GenPart[events.GenPart.pdgId == 9000025]
    rounded_radion_mass = ak.round(radions.mass, 2)

    n_radion_masses = len(set(ak.flatten(rounded_radion_mass)))
    if n_radion_masses == 1:
        return True
    else:
        print(f"Failed: File {file} has {n_radion_masses} unique radion masses.")
        print("Radion masses:", set(ak.flatten(rounded_radion_mass)))
        return False

# Class to handle datasets for coffea analysis, which caches the preprocessing steps
class CoffeaDataset(Dataset):
    
    @property
    def fileset_cache(self):
        if self.kwargs.get('use_ceph', False):
            cache_dir = ceph_cache
        else:
            cache_dir = vast_cache
        return f"{cache_dir}/{self.name}.json"

    def cache_fileset(self, fileset):
        _cache = self.fileset_cache
        if not os.path.exists(os.path.dirname(_cache)):
            os.makedirs(os.path.dirname(_cache))
        
        with open(_cache, 'w') as f:
            json.dump(fileset, f, indent=4, separators=(',', ':'))

    def fileset_base(self):

        fileset_base = {'files': {},}# 'year': self.year}
        for f in self.files:
            if self.kwargs.get('use_ceph', False):
                f = f.replace("/cms/cephfs/data", "root://hactar01.crc.nd.edu/")
                # f = f.replace("/cms/cephfs/data", "")
            # fileset_base['files'][f] = {
            #     'object_path': 'Events',
            # }
            fileset_base['files'][f] = "Events"

        return fileset_base

    def fileset(self, step_size=None, **test_args):

        if os.path.exists(self.fileset_cache):
            self.preprocessed = True
            _fileset = json.load(open(self.fileset_cache))
            _fileset['year'] = self.year
        else:
            raise ValueError(f"Fileset for {self.name} not found.")
        
        # Limit number of files for testing
        if 'n_test_files' in test_args:
            
            if test_args['n_test_files'] <= 0:
                max_files = len(_fileset['files'])
            else:
                max_files = min(test_args['n_test_files'], len(_fileset['files']))
            print(f"Limiting number of files to {max_files}")
            _fileset['files'] = {f: _fileset['files'][f] for f in list(_fileset['files'])[:max_files]}

        # Split steps into chunks of size step_size
        if step_size is not None:
            for filename, file_info in _fileset['files'].items():
                n = file_info['num_entries']
                if 'n_test_steps' in test_args:
                    n = min(n, step_size*test_args['n_test_steps'])

                file_info['steps'] = [[i, min(i+step_size,n)] for i in range(0,n+1, step_size-1)]
            
        return _fileset

class CoffeaDatasets(Datasets):
    dataset_class = CoffeaDataset
    
    def filesets(self, step_size=None, scheduler=None, preprocess=True, **test_args):

        need_to_preprocess = { d.name: d.fileset_base() for d in self if not os.path.exists(d.fileset_cache)}
        if preprocess == False:
            return need_to_preprocess
        if need_to_preprocess:
            print(f"Preprocessing {len(need_to_preprocess)} datasets")

            from coffea.dataset_tools import preprocess
            if scheduler:
                available_fileset, _ = preprocess(
                    need_to_preprocess,
                    skip_bad_files=True,
                    save_form=True,
                    scheduler=scheduler
                )
            else:
                available_fileset, _ = preprocess(
                    need_to_preprocess,
                    skip_bad_files=True,
                    save_form=True,
                )
            
            for dataset_name, fileset in available_fileset.items():
                self[dataset_name].cache_fileset(fileset)

        return {d.name: d.fileset(step_size=step_size, **test_args) for d in self}
   


# Modify NanoAOD schema to include MLPhoton object
import awkward
from coffea.nanoevents.methods import base, candidate, vector
from coffea.nanoevents.methods.nanoaod import behavior

behavior.update(awkward._util.copy_behaviors("PtEtaPhiMCandidate", "MLPhoton", behavior))

@awkward.mixin_class(behavior)
class MLPhoton(candidate.PtEtaPhiMCandidate, base.NanoCollection, base.Systematic):
    """MLNanoAOD MLPhoton Object"""
    
    @property
    def charge(self):
        return 0

    def __repr__(self):
        return "MLPhoton"

MLPhotonArray.ProjectionClass2D = vector.TwoVectorArray  # noqa: F821
MLPhotonArray.ProjectionClass3D = vector.ThreeVectorArray  # noqa: F821
MLPhotonArray.ProjectionClass4D = MLPhotonArray  # noqa: F821
MLPhotonArray.MomentumClass = vector.LorentzVectorArray  # noqa: F821


from coffea.nanoevents import NanoAODSchema
class MLNanoAODSchema(NanoAODSchema):
    warn_missing_crossrefs = False
    mixins = {
        **NanoAODSchema.mixins,
        "MLPhoton": "MLPhoton",
    }

    @classmethod
    def behavior(cls):
        """Behaviors necessary to implement this schema"""
        return behavior
