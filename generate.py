import pandas as pd
import numpy as np
import datetime
import time
import io
import os
import coconnect

from coconnect.cdm import CommonDataModel
from coconnect.cdm import define_table
from coconnect.cdm.objects.common import DestinationTable, DestinationField

class Demographics(DestinationTable):
    name = 'Demo'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.Age = DestinationField(dtype="Integer", required=False)
        self.Sex = DestinationField(dtype="Text50", required=False )
        self.Ethnicity = DestinationField(dtype="Text50", required=False )
        self.IMD = DestinationField(dtype="Integer", required=False )
        super().__init__(self.name,type(self).__name__)

class Symptoms(DestinationTable):
    name = 'Symptoms'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.date_occurrence = DestinationField(dtype="Timestamp", required=False)

        self.Positive_PCR = DestinationField(dtype="Text50", required=False )
        self.Positive_Lateral_Flow = DestinationField(dtype="Text50", required=False )
        
        self.Headache = DestinationField(dtype="Text50", required=False )
        self.Fatigue = DestinationField(dtype="Text50", required=False )
        self.Dizzy = DestinationField(dtype="Text50", required=False )
        self.Cough = DestinationField(dtype="Text50", required=False )
        self.Fever = DestinationField(dtype="Text50", required=False )
        self.Muscle_Pain = DestinationField(dtype="Text50", required=False )
        super().__init__(self.name,type(self).__name__)
        
class GP_Records(DestinationTable):
    name = 'GP_Records'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.date_of_visit = DestinationField(dtype="Timestamp", required=False)                                                                                     
        self.comorbidity = DestinationField(dtype="Text50", required=False )
        self.comorbidity_value = DestinationField(dtype="Float", required=False )
        super().__init__(self.name,type(self).__name__)

class Hospital_Visit(DestinationTable):
    name = 'Hospital_Visit'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.admission_date = DestinationField(dtype="Timestamp", required=False)   
        self.reason = DestinationField(dtype="Text50", required=False )
        super().__init__(self.name,type(self).__name__)
        
class Blood_Test(DestinationTable):
    name = 'Blood_Test'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.date_taken = DestinationField(dtype="Timestamp", required=False)   
        self.location = DestinationField(dtype="Text50", required=False )
        self.quantity = DestinationField(dtype="Float", required=False )
        super().__init__(self.name,type(self).__name__)

class Vaccinations(DestinationTable):
    name = 'Vaccinations'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.date_of_vaccination = DestinationField(dtype="Timestamp", required=False)                
        self.type = DestinationField(dtype="Text50", required=False)
        self.stage = DestinationField(dtype="Integer", required=False)
        super().__init__(self.name,type(self).__name__) 
        
        
class Serology(DestinationTable):
    name = 'Serology'
    def __init__(self,name=None,**kwargs):
        self.ID = DestinationField(dtype="Text50", required=True)
        self.Date = DestinationField(dtype="Timestamp", required=True)
        self.IgG = DestinationField(dtype="Float", required=False )
        super().__init__(self.name,type(self).__name__)


def create_gaus_time_series(mu,sigma,n):
    mu = time.mktime(mu.timetuple())
    sigma = (datetime.timedelta(**sigma)).total_seconds()
    return pd.Series([datetime.date.fromtimestamp(x) for x in np.random.normal(mu,sigma,n)])


class ExampleCovid19DataSet(CommonDataModel):
    def __init__(self,n=1000,istart=1,output_folder='./data/',chunksize=None):
        """
        initialise the inputs and setup indexing
        """
        outputs = coconnect.tools.create_csv_store(output_folder=output_folder)#,sep=)
        #outputs = coconnect.tools.create_sql_store(connection_string="postgresql://localhost:5432/ExampleCOVID19DataSet")
        os.makedirs(output_folder,exist_ok=True)

        #create people indexes that we can use in the different tables
        people = pd.DataFrame((f'pk{ii}' for ii in range(istart,n+istart)),columns=['pks'])
        #people = pd.DataFrame((ii for ii in range(1,n+1)),columns=['pks'])
        people.to_csv(f"{output_folder}/pks.csv")
        self.logger.info("created people ")
        self.inputs = coconnect.tools.load_csv([f'{output_folder}/pks.csv'],chunksize=chunksize)

        super().__init__(outputs=outputs,format_level=0,use_profiler=False)
        
        #set the processing order, e.g we want to build demographics table first
        #so that the values recorded in other tables can be demographically dependent 
        self.set_execution_order([
            'Demographics', 
            'GP_Records', 
            'Vaccinations',
            'Serology',
            'Symptoms',
            'Hospital_Visit',
            'Blood_Test'
        ])
        #process simultaneously
        self.process_simult(conserve_memory=True)
        self.close()
        
    @define_table(Demographics)
    def demo(self):  
        """
        Straight foreward demographics
        """
        self.ID.series = self.inputs['pks.csv'].reset_index()['pks']#self.cdm.people['pks']
        self.n = len(self.ID.series)
        self.Age.series = pd.Series(np.random.normal(60,20,self.n)).astype(int)
        self.Age.series = self.Age.series.mask(self.Age.series < 0 , None)
        self.Sex.series = pd.Series(np.random.choice(['Male','Female',None],size=self.n,p=[0.55,0.445,0.005]))
        self.Ethnicity.series = pd.Series(np.random.choice(['White','Black','Asian',None],size=self.n,p=[0.7,0.1,0.1,0.1]))

        def get_p_from_ethnicity(x):
            _map = {'White':0.45,'Black':0.55,'Asian':0.52,None:0.5}
            return _map[x]
        
        self.IMD.series = self.Ethnicity.series.apply(lambda x: np.random.binomial(10,p=get_p_from_ethnicity(x)))
        
    @define_table(Symptoms)
    def symptoms(self):

        #30% of people have recorded symptoms
        ID = self.cdm.demo.ID.series.sample(frac=0.3)

        #10% of these people have have multiple symptoms recorded
        self.ID.series = ID.sample(frac=1.1,replace=True)\
            .sort_values().reset_index(drop=True)  

        nsymptoms = len(self.ID.series)
        self.date_occurrence.series = create_gaus_time_series(mu=datetime.datetime(2021,1,1),
                                                              sigma={'days':365},
                                                              n=nsymptoms)
        
        self.date_occurrence.series.loc[self.date_occurrence.series.sample(frac=0.005).index] = np.nan
        
        syms_probs = {'Positive_PCR':1,'Positive_Lateral_Flow':0.99,'Headache':0.8,'Fatigue':0.7,'Dizzy':0.4,'Cough':0.7,'Fever':0.2,'Muscle_Pain':0.1}
        for key,p in syms_probs.items():
            series = pd.Series(np.random.choice(['Yes','No'],size=nsymptoms,p=[p,1-p]))
            setattr(getattr(self,key),'series',series)


    @define_table(Serology)
    def serology(self):
        
        def calc_IgG(age,sex,imd,nrisks):
            scale = 50*(1 - age/200)*(1.1 if sex=='Female' else 1.0)*(1/nrisks)
            scale = (1 - ((imd+0.1-5)/10) )*scale
            return np.random.exponential(scale=scale)
        
        df_gp = self.cdm.gp.get_df()
        df_nrisks = df_gp['comorbidity'].groupby(df_gp.index)\
                    .count()
        df_nrisks.name ='nrisks'

        df = self.cdm.demo.get_df().join(df_nrisks).reset_index()
        df['nrisks'] = df['nrisks'].fillna(1)
        df = df[df['Age']>18].sample(frac=0.3)
        nstudies = len(df)

        df = df.sample(frac=1.4,replace=True).reset_index()

        df['IgG'] = df.apply(lambda x : calc_IgG(x.Age,x.Sex,x.IMD,x.nrisks),axis=1)
        df.sort_values('ID',inplace=True)
        
        self.IgG.series = df['IgG']
        self.ID.series = df['ID']
        self.Date.series = create_gaus_time_series(mu=datetime.datetime(2021,5,1),
                                                              sigma={'days':365},
                                                              n=len(df))
    @define_table(GP_Records)
    def gp(self):
    
        def calc_comoribidites(age,imd):
            if pd.isna(age):
                return []   
            comorbidities = {
                'Mental Health':0.3*(1 + age/90 + imd/100) ,
                'Diabetes Type-II':0.15*(1 + age/70 + imd/100) ,
                'Heart Condition':0.1*(1 + age/50 + imd/100) ,
                'High Blood Pressure':0.07*(1 + age/60 + imd/100),
                'BMI': 1
            }
            return [x for x,p in comorbidities.items() if np.all(np.random.uniform() < p) ]
        
        #90% of people have a GP visit record
        df = self.cdm.demo.get_df().sample(frac=0.9).reset_index()

        df['comorbidity'] = df.apply(lambda x: calc_comoribidites(x.Age,x.IMD),axis=1)
        df['date_of_observation'] = create_gaus_time_series(mu=datetime.datetime(2010,5,1),
                                                              sigma={'days':700},
                                                              n=len(df))
        
        df = df.explode('comorbidity').set_index('ID').sort_index()
    
        self.ID.series = df.index.to_series()
        self.comorbidity.series = df['comorbidity']
        self.comorbidity_value.series = df['comorbidity'].apply(lambda x: np.random.exponential(scale=20)
                                                                if x == 'BMI' else 1)
        self.date_of_visit.series = df['date_of_observation']

    @define_table(Hospital_Visit) 
    def hospital(self):
        
        n = len(self.cdm.demo.ID.series)
        
        #5% of people have had a hospital visit
        #some of those have multiple visists
        self.ID.series = self.cdm.demo.ID.series.sample(n)\
                        .sample(int(n*1.2),replace=True)\
                        .sort_values().reset_index(drop=True)  
        
        n = len(self.ID.series)
        self.admission_date.series = create_gaus_time_series(mu=datetime.datetime(2020,5,1),
                                                              sigma={'days':300},
                                                              n=n)

        reasons = {
            'Kidney Operation':0.1,
            'Appendix Operation':0.1,
            'Heart Attack':0.2,
            'COVID-19':0.15,
            'Pneumonia':0.15,
            'Cancer':0.3
        }

        self.reason.series = pd.Series(np.random.choice(list(reasons.keys()),size=n,p=list(reasons.values())))

    @define_table(Blood_Test)
    def bloods(self):
        #half of the people with hospital visits have blood taken
        df_hospital = self.cdm.hospital.get_df().sample(frac=0.5).reset_index()
        
        self.ID.series = df_hospital['ID']
        self.date_taken.series = pd.to_datetime(df_hospital['admission_date']) \
                               + datetime.timedelta(days=np.random.uniform(0,5))
        
        n = len(df_hospital)
        self.location.series = pd.Series(np.random.choice(['Right Arm','Left Arm','Small Intestine','Abdominal Wall'],
                                                   size=n,
                                                   p=[0.3,0.3,0.2,0.2]))
        self.quantity.series = pd.Series((np.random.exponential(scale=1.5) for _ in range(0,n)))

    @define_table(Vaccinations)
    def first_covid_vaccination(self):
        
        def calc_date_of_vacc(age):
            if pd.isna(age):
                return np.nan
            start_date = datetime.datetime(2021,1,1)
            tdelta = datetime.timedelta(days=(300-age*2)+np.random.uniform(0,50))
            
            return start_date + tdelta
            
        #95% of people have had a vaccination
        df = self.cdm.demo.get_df().sample(frac=0.9).reset_index()
        
        self.ID.series = df['ID']
        self.date_of_vaccination.series =  df.apply(lambda x : calc_date_of_vacc(x.Age),axis=1)
        n = len(self.ID.series)
        self.type.series = pd.Series(np.random.choice(['Moderna','AstraZenica','Pfizer'],size=n,p=[0.34,0.33,0.33]))
        self.stage.series = pd.Series((0 for _ in range(0,n)))

    @define_table(Vaccinations)
    def second_covid_vaccination(self):
        
        def calc_date_of_vacc(age):
            if pd.isna(age):
                return np.nan
            start_date = datetime.datetime(2021,1,1)
            tdelta = datetime.timedelta(days=(300-age*2)+np.random.uniform(0,50))
            
            return start_date + tdelta
            
        #80% of people who had 1st had 2nd
        df = self.cdm.first_covid_vaccination.get_df().sample(frac=0.8).reset_index()
        
        self.ID.series = df['ID']
        self.date_of_vaccination.series =  pd.to_datetime(df['date_of_vaccination']) \
                                           + datetime.timedelta(days=(50+np.random.uniform(0,50)))
        n = len(self.ID.series)
        self.type.series = pd.Series(np.random.choice(['Moderna','AstraZenica','Pfizer'],size=n,p=[0.34,0.33,0.33]))
        self.stage.series = pd.Series((1 for _ in range(0,n)))


if __name__ == "__main__":
    #first part is 100k people with person ids from pk1-pk100000
    #ExampleCovid19DataSet(n=100000,istart=1,output_folder='./data/part1/')
    #second part is 40k people with person ids from pk100001-pk140000
    #ExampleCovid19DataSet(n=40000,istart=100001,output_folder='./data/part2/')
    #third part is 30k people with person ids that overlap with the first two parts
    # - these can be used to check what happens when duplicated are created
    # - or the demographics file can be removed and it can be used as a data dump test
    ExampleCovid19DataSet(n=30000,istart=80001,output_folder='./data/part3/')
