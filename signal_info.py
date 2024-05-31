from typing import List, Union

## Signal mass grid and functions
mass_grids = {
    'old' : {
        'M_BKK' : [180, 250, 500, 1000, 3000],
        'MOE' : [0.04, 0.02, 0.01, 0.005, 0.0025]
    },
    'current' : {
        'M_BKK' : [180, 250, 500, 1000, 1500, 2000, 2500, 3000],
        'MOE' : [0.04, 0.02, 0.01, 0.005, 0.0025]
    },
}

# Calculate mass grid from list of BKK masses and MOEs
def get_mass_grid(version):
    '''Get mass grid from list of BKK masses and MOEs'''
    BKKs = mass_grids[version]['M_BKK']
    MOEs = mass_grids[version]['MOE']
    return [SignalPoint(M_BKK=M_BKK, MOE=MOE) for M_BKK in BKKs for MOE in MOEs]


class SignalPoint:
    tag = 'BkkToGRadionToGGG'

    def __init__(
        self,
        M_BKK: Union[int, float] = None,
        M_R: Union[int, float] = None,
        MOE: Union[int, float] = None,
        tag: str = None,
        ):

        if tag is not None:
            M_BKK, M_R = self.from_tag(tag)

        assert M_BKK is not None and (M_R is not None or MOE is not None), "Must specify M_BKK and either M_R or MOE"
        assert M_R is None or MOE is None, "Cannot specify both M_R and MOE"

        self.M_BKK = M_BKK

        if M_R is not None:
            self.M_R = M_R
            self.MOE = round(M_R/(M_BKK/2), 4)
        else:
            self.M_R = round((M_BKK/2)*MOE, 4)
            self.MOE = MOE

    def from_tag(self, tag):
        import re

        M_BKK, M_R = None, None
        if 'M' not in tag or 'R0' not in tag:
            raise ValueError('Fragment does not contain mass point')
        else:
            M_BKK = float(
                re.search(r'M1-(\d+p\d+|\d+)', tag
                ).group(1).replace('p', '.'))

            M_R = float(
                re.search(r'R0-(\d+p\d+|\d+)', tag
                ).group(1).replace('p', '.'))
        
        return M_BKK, M_R

    
    def name(decimal=False):
        M_BKK = self.M_BKK
        M_R = self.M_R

        # Remove decimal if integer
        if M_BKK/int(M_BKK) == 1:
            M_BKK = int(M_BKK)
        if int(M_R) != 0 and M_R/int(M_R) == 1:
            M_R = int(M_R)
        
        tag = f'{self.tag}_M1-{M_BKK}_R0-{M_R}'
        if decimal == False:
            tag = tag.replace('.', 'p')

        return tag