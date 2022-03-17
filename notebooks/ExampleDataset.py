from coconnect.cdm import define_person, define_condition_occurrence, define_visit_occurrence, define_measurement, define_observation, define_drug_exposure
from coconnect.cdm import CommonDataModel
import json

class ExampleDataset(CommonDataModel):
    
    def __init__(self,**kwargs):
        """ 
        initialise the inputs and setup indexing 
        """
        super().__init__(**kwargs)
        
    
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
                "Male": 8507
            }
        )
        self.gender_source_concept_id.series = self.gender_source_concept_id.series.map(
            {
                "Male": 8507
            }
        )
        
    @define_person
    def person_1(self):
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
                "Female": 8532
            }
        )
        self.gender_source_concept_id.series = self.gender_source_concept_id.series.map(
            {
                "Female": 8532
            }
        )
        
    @define_observation
    def observation_0(self):
        """
        Create CDM object for observation
        """
        self.observation_concept_id.series = self.inputs["Serology.csv"]["IgG"]
        self.observation_datetime.series = self.inputs["Serology.csv"]["Date"]
        self.observation_source_concept_id.series = self.inputs["Serology.csv"]["IgG"]
        self.observation_source_value.series = self.inputs["Serology.csv"]["IgG"]
        self.person_id.series = self.inputs["Serology.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.observation_concept_id.series = self.tools.make_scalar(self.observation_concept_id.series,4288455)
        self.observation_source_concept_id.series = self.tools.make_scalar(self.observation_source_concept_id.series,4288455)
        
    @define_observation
    def observation_1(self):
        """
        Create CDM object for observation
        """
        self.observation_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_datetime.series = self.inputs["Hospital_Visit.csv"]["admission_date"]
        self.observation_source_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_source_value.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.person_id.series = self.inputs["Hospital_Visit.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.observation_concept_id.series = self.observation_concept_id.series.map(
            {
                "Heart Attack": 4059317
            }
        )
        self.observation_source_concept_id.series = self.observation_source_concept_id.series.map(
            {
                "Heart Attack": 4059317
            }
        )
        
    @define_observation
    def observation_2(self):
        """
        Create CDM object for observation
        """
        self.observation_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_datetime.series = self.inputs["Hospital_Visit.csv"]["admission_date"]
        self.observation_source_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_source_value.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.person_id.series = self.inputs["Hospital_Visit.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.observation_concept_id.series = self.observation_concept_id.series.map(
            {
                "COVID-19": 37311065
            }
        )
        self.observation_source_concept_id.series = self.observation_source_concept_id.series.map(
            {
                "COVID-19": 37311065
            }
        )
        
    @define_observation
    def observation_3(self):
        """
        Create CDM object for observation
        """
        self.observation_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_datetime.series = self.inputs["Hospital_Visit.csv"]["admission_date"]
        self.observation_source_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.observation_source_value.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.person_id.series = self.inputs["Hospital_Visit.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.observation_concept_id.series = self.observation_concept_id.series.map(
            {
                "Cancer": 40757663
            }
        )
        self.observation_source_concept_id.series = self.observation_source_concept_id.series.map(
            {
                "Cancer": 40757663
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_0(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Headache"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Headache"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Headache"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 378253
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 378253
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_1(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Fatigue"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Fatigue"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Fatigue"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 4223659
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 4223659
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_2(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Dizzy"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Dizzy"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Dizzy"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 4223938
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 4223938
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_3(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Cough"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Cough"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Cough"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 254761
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 254761
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_4(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Fever"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Fever"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Fever"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 437663
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 437663
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_5(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Symptoms.csv"]["Muscle_Pain"]
        self.condition_end_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.condition_source_concept_id.series = self.inputs["Symptoms.csv"]["Muscle_Pain"]
        self.condition_source_value.series = self.inputs["Symptoms.csv"]["Muscle_Pain"]
        self.condition_start_datetime.series = self.inputs["Symptoms.csv"]["date_occurrence"]
        self.person_id.series = self.inputs["Symptoms.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Yes": 442752
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Yes": 442752
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_6(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.condition_end_datetime.series = self.inputs["Hospital_Visit.csv"]["admission_date"]
        self.condition_source_concept_id.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.condition_source_value.series = self.inputs["Hospital_Visit.csv"]["reason"]
        self.condition_start_datetime.series = self.inputs["Hospital_Visit.csv"]["admission_date"]
        self.person_id.series = self.inputs["Hospital_Visit.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Pneumonia": 255848
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Pneumonia": 255848
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_7(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_end_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.condition_source_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_source_value.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_start_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.person_id.series = self.inputs["GP_Records.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Mental Health": 4131548
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Mental Health": 4131548
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_8(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_end_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.condition_source_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_source_value.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_start_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.person_id.series = self.inputs["GP_Records.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Mental Health": 432586
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Mental Health": 432586
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_9(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_end_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.condition_source_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_source_value.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_start_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.person_id.series = self.inputs["GP_Records.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Diabetes Type-II": 201826
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Diabetes Type-II": 201826
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_10(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_end_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.condition_source_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_source_value.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_start_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.person_id.series = self.inputs["GP_Records.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "Heart Condition": 4185932
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "Heart Condition": 4185932
            }
        )
        
    @define_condition_occurrence
    def condition_occurrence_11(self):
        """
        Create CDM object for condition_occurrence
        """
        self.condition_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_end_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.condition_source_concept_id.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_source_value.series = self.inputs["GP_Records.csv"]["comorbidity"]
        self.condition_start_datetime.series = self.inputs["GP_Records.csv"]["date_of_visit"]
        self.person_id.series = self.inputs["GP_Records.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.condition_concept_id.series = self.condition_concept_id.series.map(
            {
                "High Blood Pressure": 316866
            }
        )
        self.condition_source_concept_id.series = self.condition_source_concept_id.series.map(
            {
                "High Blood Pressure": 316866
            }
        )
        
    @define_drug_exposure
    def drug_exposure_0(self):
        """
        Create CDM object for drug_exposure
        """
        self.drug_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_exposure_end_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_exposure_start_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_source_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_source_value.series = self.inputs["Vaccinations.csv"]["type"]
        self.person_id.series = self.inputs["Vaccinations.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.drug_concept_id.series = self.drug_concept_id.series.map(
            {
                "Moderna": 35894915
            }
        )
        self.drug_source_concept_id.series = self.drug_source_concept_id.series.map(
            {
                "Moderna": 35894915
            }
        )
        
    @define_drug_exposure
    def drug_exposure_1(self):
        """
        Create CDM object for drug_exposure
        """
        self.drug_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_exposure_end_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_exposure_start_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_source_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_source_value.series = self.inputs["Vaccinations.csv"]["type"]
        self.person_id.series = self.inputs["Vaccinations.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.drug_concept_id.series = self.drug_concept_id.series.map(
            {
                "AstraZenica": 35894915
            }
        )
        self.drug_source_concept_id.series = self.drug_source_concept_id.series.map(
            {
                "AstraZenica": 35894915
            }
        )
        
    @define_drug_exposure
    def drug_exposure_2(self):
        """
        Create CDM object for drug_exposure
        """
        self.drug_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_exposure_end_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_exposure_start_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_source_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_source_value.series = self.inputs["Vaccinations.csv"]["type"]
        self.person_id.series = self.inputs["Vaccinations.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.drug_concept_id.series = self.drug_concept_id.series.map(
            {
                "Pfizer": 35894915
            }
        )
        self.drug_source_concept_id.series = self.drug_source_concept_id.series.map(
            {
                "Pfizer": 35894915
            }
        )
        
    @define_drug_exposure
    def drug_exposure_3(self):
        """
        Create CDM object for drug_exposure
        """
        self.drug_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_exposure_end_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_exposure_start_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_source_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_source_value.series = self.inputs["Vaccinations.csv"]["type"]
        self.person_id.series = self.inputs["Vaccinations.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.drug_concept_id.series = self.drug_concept_id.series.map(
            {
                "Moderna": 37003518
            }
        )
        self.drug_source_concept_id.series = self.drug_source_concept_id.series.map(
            {
                "Moderna": 37003518
            }
        )
        
    @define_drug_exposure
    def drug_exposure_4(self):
        """
        Create CDM object for drug_exposure
        """
        self.drug_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_exposure_end_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_exposure_start_datetime.series = self.inputs["Vaccinations.csv"]["date_of_vaccination"]
        self.drug_source_concept_id.series = self.inputs["Vaccinations.csv"]["type"]
        self.drug_source_value.series = self.inputs["Vaccinations.csv"]["type"]
        self.person_id.series = self.inputs["Vaccinations.csv"]["ID"]
        
        # --- insert field operations --- 
        
        # --- insert term mapping --- 
        self.drug_concept_id.series = self.drug_concept_id.series.map(
            {
                "Pfizer": 37003436
            }
        )
        self.drug_source_concept_id.series = self.drug_source_concept_id.series.map(
            {
                "Pfizer": 37003436
            }
        )
        

if __name__ == '__main__':
    ExampleDataset()
