from coconnect.cdm import define_person, define_condition_occurrence, define_visit_occurrence, define_measurement, define_observation, define_drug_exposure
from coconnect.cdm import CommonDataModel
from coconnect.tools import load_csv,create_csv_store
import json
import glob
import pandas as pd

class ExampleDataset(CommonDataModel):
    
    def __init__(self,**kwargs):
        """ 
        initialise the inputs and setup indexing 
        """
        inputs = load_csv(glob.glob('../data/part1/*'))
        outputs = create_csv_store(output_folder="./data_tests/",
                                                   sep="\t",
                                                   write_separate=True,
                                                   write_mode='w')
        
        super().__init__(inputs=inputs,outputs=outputs,**kwargs)
        self.process()
    
    @define_person
    def person_0(self):
        """
        Create CDM object for person
        """
        self.birth_datetime.series = self.inputs["Demographics.csv"]["Age"]
        self.gender_concept_id.series = self.inputs["Demographics.csv"]["Sex"]
        self.gender_source_concept_id.series = self.inputs["Demographics.csv"]["Sex"]
        self.gender_source_value.series = self.inputs["Demographics.csv"]["Sex"]
        self.person_id.series = self.inputs["Demographics.csv"]["ID"]
        
        # --- insert field operations --- 
        self.birth_datetime.series = self.tools.get_datetime_from_age(self.birth_datetime.series)
        
        # --- insert term mapping --- 
        self.gender_concept_id.series = self.gender_concept_id.series.map(
            {
                "Male": 8507,
                "Female": 8532
            }
        )

    @define_observation
    def observation_0(self):
        """
        Create CDM object for observation
        """

        def convert_igg(x):
            factor = 1.2 if x['Date'].year < 2021 else 1
            return float(x['IgG'])*factor
        
        self.observation_source_value.series = self.inputs["Serology.csv"]["IgG"]

        self.inputs["Serology.csv"]["Date"] =  pd.to_datetime(self.inputs["Serology.csv"]["Date"])
        self.inputs["Serology.csv"]["IgG"] = self.inputs["Serology.csv"].apply(lambda x: convert_igg(x),axis=1)
        self.inputs["Serology.csv"]["Units"] = 'g/L'
        self.unit_source_value.series = self.inputs["Serology.csv"]["Units"]
        self.value_as_number.series = self.inputs["Serology.csv"]["IgG"]

        
        self.observation_concept_id.series = self.inputs["Serology.csv"]["IgG"]
        self.observation_datetime.series = self.inputs["Serology.csv"]["Date"]
        self.observation_source_concept_id.series = self.inputs["Serology.csv"]["IgG"]
        self.person_id.series = self.inputs["Serology.csv"]["ID"]

        
        # --- insert term mapping --- 
        self.observation_concept_id.series = self.tools.make_scalar(self.observation_concept_id.series,4288455)
        self.observation_source_concept_id.series = self.tools.make_scalar(self.observation_source_concept_id.series,4288455)
        
        

if __name__ == '__main__':
    ExampleDataset()
