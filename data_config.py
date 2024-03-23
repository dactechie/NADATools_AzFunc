from models.categories import Purpose
from pandas.api.types import CategoricalDtype

keep_parent_fields = ['Program', 'Staff', 'AssessmentDate', 'SLK'] # if also in SurveyData 
# {
#   'Matching':['Program', 'Staff']
# }

ATOM_DB_filters = {
  # extract purpose
   Purpose.NADA : {
    'Program' : ('SAPPHIRE','EUROPATH','MONPATH','BEGAPATH'
                 ,'MURMICE','GOLBGNRL','GOLBICE','MURMPP','MURMWIO','MURMHEAD',) 
   },
   Purpose.MATCHING:{}
   
  }
# range
# remove_if_under_threshold = {
#      'PDCHowMuchPerOccasion' : {
#           'Other': 2 # if under 2 percent of dataset, delete records
#      }
# }
category_yes_no = CategoricalDtype(['Yes', 'No'])

# TODO Fix:
# Past4WkBeenArrested :'No', 'Yes', 'Yes - please provide details'

# df.Past4WkAodRisks[df.Past4WkAodRisks.notna()]
# 3       [Driving with drugs and/or alcohol in your sys...
# 8                    [Using more than one drug at a time]
# 311       [Memory Loss, Using Alone, Violence / Assault]
# 7312                                        [Using Alone]
# 7314    [Using Alone, Driving with drugs and/or alcoho...
# HaveAnySocialSupport
# Some            1698
# A few           1583
# Quite a lot     1181
# A wide range     668
# None             276

category_feel_safe = CategoricalDtype(['Yes - Completely safe',
       'Mostly safe. Sometimes feel threatened',
       'Often feel unsafe / Occasionally experience violence',
       'Never feel safe / Constantly exposed to violence'      ])

category_notatall_to_daily = CategoricalDtype(['Not at all'
                                               ,  'Less than weekly'
                                               , 'Once or twice per week'
                                               , 'Three or four times per week'
                                               , 'Daily or almost daily'], ordered=True)

# HowSatisfiedWithProgress
category_notatall_to_extremely = CategoricalDtype([
        'Not at all ' , 'Slightly' , 'Moderately'
        , 'Considerably' , 'Extremely'], ordered=True)


predef_categories = {
    'Past4WkDailyLivingImpacted': category_notatall_to_daily,
    'Past4WkUseLedToProblemsWithFamilyFriend':category_notatall_to_daily,
  'Past4WkHowOftenIllegalActivities':       category_notatall_to_daily,
  'Past4WkHowOftenMentalHealthCausedProblems': category_notatall_to_daily,
  'Past4WkHowOftenPhysicalHealthCausedProblems': category_notatall_to_daily, 
  'Past4WkDifficultyFindingHousing': category_notatall_to_daily,
  'HowSatisfiedWithProgress': category_notatall_to_extremely,
  'DoYouFeelSafeWhereYouLive': category_feel_safe,
  'Past4WkHadCaregivingResponsibilities': category_yes_no,
  'Past4WkBeenHospCallAmbulance': category_yes_no,
  'Past4WkAnyOtherAddictiveB': category_yes_no,
}

question_list_for_categories = [
  'Program',
  'Staff',
  'SurveyName',
  'AssessmentType',

  'IndigenousStatus',
  'ClientType',
  'CountryOfBirth',
  'LivingArrangement',

  # 'PDCSubstanceOrGambling',
  # 'PDCGoals',  
  # 'PDCMethodOfUse',

  'HowImportantIsChangeToYou',
  'HaveAnySocialSupport',  
]

question_list_for_categories = question_list_for_categories + list(predef_categories.keys())


# survey_datacols_preregex = [
#    '.*_Score$'
# ]
# survey_datacols_postregex = [
#    '^Past4Wk.*'
# ]

mulselect_option_to_nadafield = {
    'Past4WkAodRisks': {'ATOPRiskEviction': "At risk of eviction",
                         'ATOPHomeless': "Homeless",
                         'Past4Wk_ViolentToYou': "Violence / Assault"
                         },
    'PrimaryCaregiver': {
      'PrimaryCaregiver_0-5':'Yes - primary caregiver: children under 5 years old'  ,
      'PrimaryCaregiver_5-15': 'Yes - primary caregiver: children 5 - 15 years old',

    },
    
    'Past4WkEngagedInOtheractivities.Paid Work': { 'PaidWorkDays' :'Days'}, 
      'Past4WkEngagedInOtheractivities.Study - college, school or vocational education':{'StudyDays':'Days'}

  }

fieldname_suffixes_range= ["PerOccassionUse"]

# replace left(key) value with the right value in dataset
option_variants = {
    'PDCMethodOfUse': {
      'Ingests': 'Ingest',
      'Injects': 'Inject',
      'Smokes': 'Smoke',
    },
    'Past4WkDailyLivingImpacted': {
        'Once or twice a week' : 'Once or twice per week',
        'Three or four times a week': 'Three or four times per week'
    },
    'DoYouFeelSafeWhereYouLive': {
       'Often feel unsafe. Occasionally experience violence': 'Often feel unsafe / Occasionally experience violence',
       'Never feel safe. Constantly exposed to violence': 'Never feel safe / Constantly exposed to violence'             
    }
}

data_types:dict = {
  'PartitionKey': 'string',
'RowKey': 'string',
'Program': 'string',
'Staff': 'string',
'AssessmentDate': 'date',
'SurveyName': 'string',

# 'PDCSubstanceOrGambling': 'string',
# 'PDCMethodOfUse': 'string',
# 'PDCDaysInLast28': 'numeric',
# 'PDCUnits': 'string',

# 'PDCHowMuchPerOccasion': 'range', # can be 'Other'/NaN/float/int/range -> convert to float64, exclude Other

'HaveYouEverInjected': 'string',
'SDSIsAODUseOutOfControl': 'numeric',
'SDSDoesMissingFixMakeAnxious': 'numeric',
'SDSHowMuchDoYouWorryAboutAODUse': 'numeric',
'SDSDoYouWishToStop': 'numeric',
'SDSHowDifficultToStopOrGoWithout': 'numeric',

'Past4WkPhysicalHealth': 'numeric',
'Past4WkMentalHealth': 'numeric',
'Past4WkQualityOfLifeScore': 'numeric',
}

# -------------------------- Results Grouping -------------------------

results_grouping = {
    "Wellbeing measures":{
        'questions': ['Past4WkPhysicalHealth', 'Past4WkMentalHealth', 'Past4WkQualityOfLifeScore'],
        'description':'Changes in average scores for "Past 4 weeks: Wellbeing measures"'
    },
    "Substance Use": {
        'questions': ['PDCHowMuchPerOccasion' , 'PDCDaysInLast28'],
        'description':'Changes in average scores for "Past 4 weeks: Substance use"'
    },
    "Problems in Life Domains": {
        'questions': ['Past4WkDailyLivingImpacted'
                     , 
                    'Past4WkHowOftenPhysicalHealthCausedProblems'
                    , 'Past4WkHowOftenMentalHealthCausedProblems'
                    , 'Past4WkUseLedToProblemsWithFamilyFriend'
                    , 'Past4WkDifficultyFindingHousing'
                    ], #Past4WkHowOftenIllegalActivities
        'description':'Changes in average scores for "Past 4 weeks: Use let to problems in various Life domains"'
    },
    "SDS and K10" : {
        'questions': ['SDS_Score', 'K10_Score'],
        'description':'Changes in average scores for "SDS & K10"'
    }
}


# --------------- Filters -----------------------------------

funder_program_grouping ={
    'Coordinaire' : ['SENSW-Pathways', 'SENSW-Sapphire'],
    'NSW Ministry of Health' : ['NSW Methamphetamine Support'],
    'Murrumbidgee PHN' : ['Murrumbidgee Pathways'],
    'ACT Health' : ['TSS', 'Arcadia']    
}

program_grouping = {
      'SENSW-Pathways':['EUROPATH','MONPATH','BEGAPATH','GOLBGNRL'],
      'SENSW-Sapphire':['SAPPHIRE'],
      'NSW Methamphetamine Support':['MURMICE', 'GOLBICE'],
      'Murrumbidgee Pathways' : ['MURMWIO', 'MURMPP','MURMHEAD'],
      'TSS': ['TSS'],
      'Arcadia': ['ARCA']
}


# ------------------- MDS Mapping  -----

# SurveyCode	ESTABLISHMENT IDENTIFIER
EstablishmentID_Program = {
    '820002000': 'TSS'
    ,'82A000004': 'ARCCOCO'
    ,'82A000004': 'ARCRESI'
    ,'12QQ03076': 'SAPPHIRE'
    ,'12QQ03062': 'EUROPATH'
    ,'12QQ03061': 'MONPATH'
    ,'12QQ03063': 'BEGAPATH'
    ,'13K034': 'MURMICE'
    ,'12QQ03022': 'GOLBGNRL'
    ,'13Q035': 'GOLBICE'
    ,'12KK03024': 'MURMPP'
    ,'12KK03025': 'MURMWIO'
    ,'12KK03023': 'MURMHEAD'
}


# ---- for NADA Drug mapping pivot

activities_w_days = {'Past4WkEngagedInOtheractivities.Paid Work': 'PaidWorkDays',
   'Past4WkEngagedInOtheractivities.Study - college, school or vocational education':'StudyDays',
    # 'Past4WkEngagedInOtheractivities.Looking after children'
  }

PDC_ODC_ATOMfield_names = {
    'ODC': {
      'drug_name' : 'OtherSubstancesConcernGambling',
      'used_in_last_4wks' : 'DaysInLast28',
      'per_occassion': 'HowMuchPerOccasion',
      'units': 'Units',
    },
    'PDC':{
      'drug_name' : 'PDCSubstanceOrGambling',
      'used_in_last_4wks' : 'PDCDaysInLast28',
      'per_occassion': 'PDCHowMuchPerOccasion',
      'units': 'PDCUnits',
    }
}

# TODO: use NADA fields
# DU Heroin use number of days
# DU Other opioid use number of days
# DU Cannabis use number of days
# DU Cocaine use number of days
# DU Amphetamine use number of days
# DU Tranquilliser use number of days
# DU Another drug use number of days
# DU Alcohol use number of days


# Define the categories and their corresponding names in the data
nada_drug_days_categories = {
    'Alcohol': ['Ethanol','Alcohols, n.e.c.'],
    'Heroin': ['Heroin'],
    'Other Opioids': ['Oxycodone','Pharmaceutical Opioids','Pharmaceutical Opioids, n.f.d.', 'Methadone', 'Opioid Antagonists, nec'],
    # Fentanyl, Tramadol, COdeine, Morphine

    'Cocaine': ['Cocaine'],
    'Cannabis': ['Cannabinoids and Related Drugs, n.f.d.', 'Cannabinoids and related drugs, n.f.d.', 'Cannabinoids'],
    'Amphetamines': ['Amphetamines, n.f.d.', 'Amphetamines, n.f.d', 'Methamphetamine','Dexamphetamine'],     
    'Benzodiazepines': ['Benzodiazepines, nec', 'Benzodiazepines, n.e.c.', 'Benzodiazepines, n.f.d', 'Benzodiazepines, n.f.d.','Diazepam' ],
    # 'Another Drug':  [
    #    'Opioid Antagonists, n.e.c.','Volatile Nitrates, n.e.c.', 'Lithium',
    #    'Other','Other Drugs of Concern', 'Psychostimulants, n.f.d.','Zolpidem', 'Caffeine',  'MDMA/Ecstasy',  
    #                     'Gamma-hydroxybutyrate','Dexamphetamine','GHB type Drugs and Analogues, n.e.c.',
    #                     'Psilocybin or Psilocin', 'Amyl nitrate', 'Other Volatile Solvents, n.e.c.' ],
    'Nicotine': ['Nicotine'],
    'Gambling':['Gambling'],
    'Another Drug1':[], # h ack for expand_drug_info to work ->  for drug_cat in nada_drug_days_categories.keys():
    'Another Drug2': [] # h ack for expand_drug_info to work ->  for drug_cat in nada_drug_days_categories.keys():
}


nada_cols_final = [
  'AgencyCode'
]

nada_cols = [
'Program' # AgencyCode
,'RowKey'  # -> episode ID
, 'PartitionKey'  # -> MDS ClientCode
,'AssessmentType' # initial -> SurveyStatge  = 0 (progress)

,'Staff' #-> # Debugging
,'AssessmentDate' #5. COMS admin date

,'SDSIsAODUseOutOfControl'      # Q6
,'SDSDoesMissingFixMakeAnxious'
,'SDSHowMuchDoYouWorryAboutAODUse'
,'SDSDoYouWishToStop'             #15    Q4/
,'SDSHowDifficultToStopOrGoWithout'  # 11     Q5. How difficult would you/did you find it to stop  

## DU 
# ,'Alcohol_DaysInLast28'
# ,'Cannabis_DaysInLast28'  # 77. ATOP 1B.3   Cannabis Total (past 4 weeks)
# ,'Heroin_DaysInLast28'
# ,'Other Opioids_DaysInLast28'
# ,'Cocaine_DaysInLast28'
# ,'Amphetamines_DaysInLast28'
# ,'Benzodiazepines_DaysInLast28'
# ,'Another Drug1_DaysInLast28'


,'K10Q01'  # 30 
,'K10Q02'
,'K10Q03'
,'K10Q04'
,'K10Q05'
,'K10Q06'
,'K10Q07'
,'K10Q08'
,'K10Q09'
,'K10Q10'
,'K10Q11'
,'K10Q12'
,'K10Q13'
,'K10Q14'  # 43    Q14. In the last four weeks, how often have physical health problems been the main cause of these feelings?

# ATOP 65 - 160
# 65. ATOP 1A.1 ATOP Alcohol Typical Qty String 50
,'Alcohol_TypicalQtyStr'
# # The number of standard drinks of alcohol ingested on a typical drinking day refer to standard drinks guide.
# # 0 to 999 along with a description, being ‘standard drinks’
# # 66  Alcohol Wk 4 - 69 - Wk1  ---- ignore
# # 70 Total Alcohol : The total number of days alcohol consumed in the past four weeks.
,'Alcohol_DaysInLast28'
# #71. ATOP 1A.4  ATOP Alcohol No Answer  ; # 0 asked -1 not asked

, 'Alcohol_PerOccassionUse' # for DU


# #72 # Cannabis Typical Qty 
# # The amount of cannabis consumed on a typical day of cannabis use in the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
# # 72. ATOP 1B.1  Cannabis Typical Qty (string) ; 0 to 999 (plus description of units)
,'Cannabis_TypicalQtyStr'
,'Cannabis_DaysInLast28'  # 77. ATOP 1B.3   Cannabis Total (past 4 weeks)

,'Heroin_TypicalQtyStr'
,'Heroin_DaysInLast28'

,'Other Opioids_TypicalQtyStr'
,'Other Opioids_DaysInLast28'

,'Cocaine_TypicalQtyStr'
,'Cocaine_DaysInLast28'

, 'Amphetamines_TypicalQtyStr'
,'Amphetamines_DaysInLast28'

,'Benzodiazepines_TypicalQtyStr'
,'Benzodiazepines_DaysInLast28'

,'Another Drug1'
,'Another Drug1_TypicalQtyStr'
# # 123. ATOP 1H.ii.1 0 to 999 (plus description of units)
# # Other Substance 2 Typical Qty The average amount of Other Substance 2 used on a typical day during the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
,'Another Drug1_DaysInLast28'

,'Another Drug2'
,'Another Drug2_TypicalQtyStr'
# # 123. ATOP 1H.ii.1 0 to 999 (plus description of units)
# # Other Substance 2 Typical Qty The average amount of Other Substance 2 used on a typical day during the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
,'Another Drug2_DaysInLast28'


# # 131 ATOP 1K.1 Daily Tobacco Use Confirmation of the client’s use of tobacco
# # 132 ATOP 1K.2  Daily Tobacco Use Typical Qty The average amount of tobacco used on a typical day during the past four weeks.
,'Nicotine_TypicalQtyStr'  # TODO: WARNING :typical day  != occassion
,'Nicotine_DaysInLast28'

, 'Nicotine_PerOccassionUse' # for DU

,'Past4WkNumInjectingDays'  # 136 ATOP 1K.2   Injected Total The total number of days injected in the past four weeks.



# ,'Past4WkEngagedInOtheractivities.Paid Work'     # 143. ATOP 2A.2 ATOP Days Paid Work Total
# # ,'Past4WkEngagedInOtheractivities.Voluntary Work'
# ,'Past4WkEngagedInOtheractivities.Study - college, school or vocational education'   # 149. ATOP 2B.2 he total number of days of school or study in the past four weeks.
, "ATOPHomeless",	"ATOPRiskEviction",	"PrimaryCaregiver_0-5",	"PrimaryCaregiver_5-15"
,'Past4WkBeenArrested' # 155  ATOP 2F Has the client been arrested over the past four weeks?

# ,'Gambling_PerOccassionUse'  ->> Question: should this be "Other drug" or  excluded all togther ?
# ,'Gambling_DaysInLast28'  ->> Question: should this be "Other drug" or  excluded all togther ?

, 'PaidWorkDays'
, 'StudyDays'
# ,'PrimaryCaregiver_0-5', 'PrimaryCaregiver_5-15'

, 'Past4Wk_ViolentToYou' # 
 # Past4WkAodRisks  ["Violence / Assault",]  # Has anyone been violent (incl. domestic violence) towards the client in past four weeks?
              #  #156.  ATOP 2G  ATOP Violent To You Has anyone been violent (incl. domestic violence) towards the client in past four weeks?

,'Past4WkHaveYouViolenceAbusive'  # ATOM:"Have you used violence or been abusive towards anyone, over the last 4 weeks?", "Yes (risk assessment required)"
         ##"Have you used violence or been abusive towards anyone, over the last 4 weeks?",:  157 ATOP 2H Violent To Others Has the client been violent (incl. domestic violence) towards someone else in the past four weeks?

,'Past4WkMentalHealth' #158  ATOP 2 I Psychological Health Status Client’s rating of their psychological wellbeing in past four weeks (anxiety, depression, problems with emotions and feelings) 0=poor 10=good
,'Past4WkPhysicalHealth'  # 159. ATOP 2J Physical Health Status Client’s rating of their physical health in past 4 weeks (extent of physical symptoms and bothered by illness)
,'Past4WkQualityOfLifeScore'  # 160.  ATOP 2K Qual of life  
]

# -1 not answered/no answer
notanswered_defaults = [
  'ATOPInjectedUsedEquipment',           ##
  'ATOPDailyTobaccoUse',                 ##
  'YourCurrentHousing_Homeless',         ##
  'YourCurrentHousing_Atriskofeviction', ##

  'PrimaryCaregiver_0-5' ,
  'PrimaryCaregiver_5-15',
  'Past4WkBeenArrested',
  
  'Past4WkAodRisks_ViolentToYou',        ##

  'Past4WkHaveYouViolenceAbusive', #  Has the client been violent 
  'Past4WkPhysicalHealth'     , #not answered/no answer
  'Past4WkMentalHealth'       ,
  'Past4WkQualityOfLifeScore' ,
]

nada_final_fields = [
 "AgencyCode","PMSEpisodeID",	"PMSPersonID",	"Stage",	"AssessmentDate",	

"PDCCode",# SDS 1.0
"SDSIsAODUseOutOfControl",	"SDSDoesMissingFixMakeAnxious",	"SDSHowMuchDoYouWorryAboutAODUse",
  	"SDSDoYouWishToStop",	"SDSHowDifficultToStopOrGoWithout",		
"2nd SDS 1.1",	"2nd SDS 1.2",	"2nd SDS 1.3",	"2nd SDS 1.4",	"2nd SDS 1.5",		
# DU ---
"Heroin_DaysInLast28",	"Other Opioids_DaysInLast28",	"Cannabis_DaysInLast28",	
  "Cocaine_DaysInLast28",	"Amphetamines_DaysInLast28", "Benzodiazepines_DaysInLast28",
  "Another Drug1_DaysInLast28","Alcohol_DaysInLast28","Alcohol_PerOccassionUse",
  "DUDrinkingmoreheavilynumberofdrinks","DUDrinkingmoreheavilynumberofdays",
  "Nicotine_DaysInLast28", "Nicotine_PerOccassionUse",

"K10Q01",	"K10Q02",	"K10Q03",	"K10Q04",	"K10Q05",	"K10Q06",	"K10Q07",	"K10Q08",	"K10Q09",	"K10Q10",		
"K10Q11",	"K10Q12",	"K10Q13",	"K10Q14",	

# blanks ---------------------------------------------------
"QoLRatequalityoflife",	"QoLRatehealth",	"QoLRateenergyforeverydaylife",	"QoLMoneytomeetneeds",
"QoLAbilitytoperformdailyactivities",	"QoLSatisfiedwithself",	"QoLSatisfiedwithpersonalrelationships",
	"QoLSatisfiedwithconditionsofyourlivingplace",  "QoLPrincipalsourceofincome(MDS)", 
  "QoLLivingarrangements(MDS)",	"QoLUsualaccommodation(MDS)",			
"QoLNumberofoccasionsarrested(BTOM)",  "QoLNumberofarrestsforrecentoffences(BTOM)",
  "BBVInjectingdruguse(BTOM)",	"BBVSharingofneedleandsyringe(BTOM)",
    	"BBVSharingotherinjectingequipment(BTOM)",	"BBVDrugoverdoses(BTOM)",		
"NDDoyousmoketobacco?",	"NDHowsoonafterwakingdoyousmokeyourfirstcigarette?",
    "NDHowmanycigarettessmokedonatypicalday?","NDwithdrawalsorcravingsexperienced?",			
#--------------------blanks

# -- ATOP
"Alcohol_TypicalQtyStr",	"ATOPAlcoholWk4",	"ATOPAlcoholWk3",	"ATOPAlcoholWk2",
      "ATOPAlcoholWk1",	"Alcohol_DaysInLast28",	"ATOPAlcoholNoAnswer",
"Cannabis_TypicalQtyStr",	"ATOPCannabisWk4",	"ATOPCannabisWk3",	"ATOPCannabisWk2",
      "ATOPCannabisWk1",	"Cannabis_DaysInLast28",	"ATOPCannabisNoAnswer",
"Amphetamines_TypicalQtyStr",	"ATOPAmphetamineWk4",	"ATOPAmphetamineWk3",	"ATOPAmphetamineWk2",
      "ATOPAmphetamineWk1",	"Amphetamines_DaysInLast28",	"ATOPAmphetamineNoAnswer",
"Benzodiazepines_TypicalQtyStr",	"ATOPBenzodiazepinesWk4",	"ATOPBenzodiazepinesWk3",	"ATOPBenzodiazepinesWk2",
    	"ATOPBenzodiazepinesWk1",	"Benzodiazepines_DaysInLast28",	"ATOPBenzodiazepinesNoAnswer",
"Heroin_TypicalQtyStr",	"ATOPHeroinWk4",	"ATOPHeroinWk3",	"ATOPHeroinWk2",
      "ATOPHeroinWk1",	"Heroin_DaysInLast28",	"ATOPHeroinNoAnswer",
"Other Opioids_TypicalQtyStr",	"ATOPOtherOpiodsWk4",	"ATOPOtherOpiodsWk3",	"ATOPOtherOpiodsWk2",
      "ATOPOtherOpiodsWk1",	"Other Opioids_DaysInLast28",	"ATOPOtherOpiodsNoAnswer",
"Cocaine_TypicalQtyStr",	"ATOPCocaineWk4",	"ATOPCocaineWk3",	"ATOPCocaineWk2",	
      "ATOPCocaineWk1",	"Cocaine_DaysInLast28",	"ATOPCocaineNoAnswer",
"Another Drug1",	"Another Drug1_TypicalQtyStr",	"ATOPOtherSubstance1Wk4",	"ATOPOtherSubstance1Wk3",	"ATOPOtherSubstance1Wk2",
    	"ATOPOtherSubstance1Wk1",	"Another Drug1_DaysInLast28",'Another Drug1_NoAnswer' ,

"Another Drug2",	"Another Drug2_TypicalQtyStr",	"ATOPOtherSubstance2Wk4",	"ATOPOtherSubstance2Wk3",	"ATOPOtherSubstance2Wk2",
    	"ATOPOtherSubstance2Wk1",	"Another Drug2_DaysInLast28",'Another Drug2_NoAnswer',

"ATOPDailyTobaccoUse",	"Nicotine_TypicalQtyStr",				##  TODO	

"ATOPInjectedWk4",	"ATOPInjectedWk3",	"ATOPInjectedWk2",	"ATOPInjectedWk1",
    	"Past4WkNumInjectingDays",	"ATOPInjectedNoAnswer",	"ATOPInjectedUsedEquipment",

"ATOPDaysPaidWorkWk4",	"ATOPDaysPaidWorkWk3",	"ATOPDaysPaidWorkWk2",	
      "ATOPDaysPaidWorkWk1",	"PaidWorkDays",	"ATOPDaysPaidWorkNoAnswer",	
"ATOPDaysEducationWk4",	"ATOPDaysEducationWk3",	"ATOPDaysEducationWk2",	
      "ATOPDaysEducationWk1",	"StudyDays",	"ATOPDaysEducationNoAnswer",

"ATOPHomeless",	"ATOPRiskEviction",	"PrimaryCaregiver_0-5",	"PrimaryCaregiver_5-15",			

"Past4WkBeenArrested",	"Past4Wk_ViolentToYou", "Past4WkHaveYouViolenceAbusive",

"Past4WkMentalHealth",	"Past4WkPhysicalHealth",	"Past4WkQualityOfLifeScore",				

]


# 12. 2nd SDS 1.1 SDS Drug use out of control against the Intake drug 
# 13. 2nd SDS 1.2 SDS Drug use missing anxious/worried against the intake drug) ???swapped field 
# 14. 2nd SDS 1.3 SDS Drug use worry about use???swapped field 
# 15. 2nd SDS 1.4 SDS Drug use wish stop 
# 16. 2nd SDS 1.5 SDS Drug use difficult to stop

# ,'PrimaryCaregiver'  # ? 153 ? ATOP 2Ei Primary Caregiver Under 5 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?
# ,'Past4WkEngagedInOtheractivities.Looking after children'  # ? 153 ? ATOP 2Ei Primary Caregiver Under 5 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?
#             #154  Primary Caregiver 5 to15 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?

# ,'Past4WkHadCaregivingResponsibilities' # 154. ATOP 2Eii  # 1 yes 0 no -1 not answered/no answer