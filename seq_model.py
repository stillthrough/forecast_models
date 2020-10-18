import pandas as pd
import numpy as np
import datetime as dt
from tkinter import *

class seq_model:
    def __init__(self, startwk, endwk = 53, subentity = 'NA', filepath = ''
                 , startyr = dt.date.today().year, yrlag = 3, wklag = 3, weightlist = []
                 , ctradj = False, seq_scenarios = 1, seq_adjtype = 'sup'):
        self.startwk = startwk #Week to start forecasting
        self.endwk = endwk #Week to end forecasting - Default to EoY 
        self.startyr = startyr #Year of start forecasting
        self.yrlag = yrlag #Years to go back and retrieve historical data
        self.wklag = wklag #Weeks to go back to calculate lagged avg
        self.subentity = subentity
        self.filepath = r'C:\Users\wsun\Desktop\Master - PythonModelExcel.xlsx' #Filepath
        self.samplewk = [x for x in range(self.startwk-wklag,self.startwk)] #To Get current lagged avg
        self.sampleyr = [x for x in range(self.startyr-yrlag,self.startyr)] #To get past year ctr
        self.pywk = [x for x in range(self.startwk-wklag,self.startwk+1)] #To get past year ctr
        self.weightlist = weightlist
        self.ctradj = ctradj
        self.seq_scenarios = seq_scenarios
        self.seq_adjtype = seq_adjtype
        
#QC Function - 
    def qc(self):
        print(self.filepath, self.yrlag, self.wklag, self.startwk, self.startyr, self.samplewk)

#TODO: Dynamically allow user to pass in overrides
#Weight Assignment - Default dictionary of weights provided if no user inputs
    def get_weight(self, lag):
        #Add in logic to allow user to input customized weights and override default:
        if self.weightlist: # If user has customized weights
            if isinstance(self.weightlist,str): #Split if taking the weights straight from users
                customlist = [float(x) for x in self.weightlist[self.weightlist.index('[')+1:\
                                                           self.weightlist.index(']')].split(',')]
            elif isinstance(self.weightlist, list): #Take it as list if testing
                customlist = self.weightlist
            if len(customlist) == lag: #Ensuring lag matches the length of provided weights
                return customlist
            else:
                print(f'Length Mismatch: You provided {len(customlist)} but doing {lag}')

        else: # If user has no customized weights
            weightlist = {
                1 : [1],
                2 : [0.5,0.5],
                3 : [0.15, 0.35, 0.5],
                4 : [0.125,0.175,0.3,0.4],
                5 : [0.05,0.1,0.2,0.25,0.4]
            }
            uselist = weightlist[lag]
            return uselist
        
#TODO: Check if part 1 needed - used to catch where user says exclusions but no params provided 
#NON-INDEPENDENT FUNCTION - Function to get exclusion year & weeks, used in calculating hist   
    def get_exclusions(self, exclusions = ''):
        #Part1: Check if exclyrwk already exist. Running local allows userinput
        if not exclusions: #If 'exclusions' doesn't exist or empty, prompt user to input
            print('Enter Year/WK Exclusions Below:\n'\
                  + 'Format: (Year:[wk1,wk2,wk3]; Year2:[wk1,wk2,wk3])')
            exclusions = input()
        #Part2: Once Start splitting components and combine them into unique yr/wk idenifier
        try:
            exclyrwk = [] #Create empty list to store identifiers
            for x in exclusions.split(';'): #Split by ; to get all input segments
                exclyr = int(x[0:x.index(':')].strip()) #Retrieve year
                weeks = x[x.index('[')+1:x.index(']')].strip() #Retrieve the block containing all WKs                 
                for week in weeks.split(','): #For all input weeks, generate key containing yr + wk
                    if ':' in week: #If users input a range of wks with [x:y] format, treat it as range
                        modweeks = [x for x in range(int(week[0:week.index(':')])\
                                                     ,int(week[week.index(':')+1:])+1)]
                        for modweek in modweeks: #Combined them with yr and input into output list
                            exclyrwk.append(int(modweek) + exclyr *100) # Year * 100 + WK Num
                    else:
                        exclyrwk.append(int(week) + exclyr*100) # Year * 100 + WK Num
            exclyrwksrt = sorted(exclyrwk)            
            return exclyrwksrt #Return output list
        except (ValueError, TypeError) as e:
            print('ERROR! Please check your input follows the format guideline!')
            raise e
        
#Fetch data from Excel storing historical data (NA & UK separate tabs) and format for manipulation
#Need to use columns for astype to work - alternative is FOR LOOP
#Subentity: Either 'NA' or 'UK', both not supported currently, need LOOP
#ctradj (Optional) = known adjustments we are making to every week
    def fetch_history(self, preview = False, exclusions = False):
        hs = pd.read_excel(self.filepath, sheet_name = f'{self.subentity}Historical'\
                           ,usecols = 'A:M', header = 0)
        hs.drop(hs[~hs['DoWNum'].isin([x for x in range(1,8)])].index, inplace = True)
        hs[['Date','EoM']] = hs[['Date','EoM']].apply(pd.to_datetime)
        hs.iloc[:,[1,2,4,5,9,10,11,12]] = hs.iloc[:,np.r_[1,2,4,5,9,10,11,12]].astype('int64')
        #hs[hs.columns[np.r_[1,2,4,5,9,10,11,12]]] = hs.iloc[:,np.r_[1,2,4,5,9,10,11,12]].astype('int64')                                                            
        #df = df.astype({colname1:type1, colname2: type2})
        if exclusions:
            excllist = self.get_exclusions()
            hs.drop(hs[(hs['Year'] * 100 + hs['WKNum']).isin(excllist)].index, inplace = True)
            
        #If need to review dataframes prior to running forecast, set to TRUE
        if preview: 
            print(hs.dtypes, hs.head(5), hs.tail(5))
        return hs

#NON-INDEPENDENT FUNCTION - Function to calculate trailing average, params passed in later
    def get_trailing_avg(self, data, wklag, weights, col_name = 'Calls'):
        travg = []
        if wklag == 1: #If week lag = 1, return actual values
            travg = data.iloc[:,np.where(data.columns == col_name)[0][0]]
            return travg
        elif wklag >= 2: # If week lag > 2, calculate trailing averages
            for x in range(len(data.iloc[:,np.where(data.columns == col_name)[0][0]])):
                if x >= wklag-1:
                    op = data.iloc[:,np.where(data.columns == col_name)[0][0]][x-(wklag-1):x+1]
                    travg.append(float(f'{(np.average(op, weights = weights)):.3f}'))
                else:
                    travg.append(np.nan)
            return travg
        else: #To Catch Non-Numerical and 0 Values
            print('ERROR! - Invalid Value, Please Try Again')
        
#Retrieving current year actuals - startwk: forecast start week, lag = lag on actuals   
    def trailing_avg(self, data):
        #Retrieve data from past few weeks in current years based on week lag
        twa = data[(data['WKNum'].isin(self.samplewk)) & (data['Year'] == self.startyr)]\
                                        .groupby(['Year','WKNum'])\
                                        .sum()[['Calls','Order(Act)','Visit(Act)']]\
                                        .reset_index()
        #Calculating the trailing averages 
        for x in [x for x in range(1,self.wklag + 1)]:
            twa[f'{x}WKLagAvg'] = self.get_trailing_avg(data = twa, wklag = x,\
                                                        weights = self.get_weight(x))
        
        #Dynamic locate all columns with 'Lag Avgs' & Reference in twa(trailing weight avg) formula
        lagavgcols = np.r_[np.where(twa.columns == '1WKLagAvg')[0][0]:len(twa.columns)]
        twastart = pd.DataFrame({twa.iloc[-1,1]:[x for x in twa.iloc[-1,lagavgcols]]}
                                       ,index = [twa.columns[lagavgcols]])
        return twastart

#NON-INDEPENDENT FUNCTION - Function to calculate historical WoW change, params passed in later
    def calculate_seq(self, data):
        data1 = data.shift(1, axis = 1) #Shifting to get value from previous week to calculate WoW
        for x in range(len(data.columns)): #Iterate through all weeks
            for y in range(len(data.index)): #Iterate through all years - 'yrlag' params
                if data.iloc[y,x] != 0 and data1.iloc[y,x] != 0: #Do division if both valid values
                    data.iloc[y,x] = data.iloc[y,x]/data1.iloc[y,x] #Abs comparision (e.g., +105%)
                else: #If invalid, set it to np.nan
                    data.iloc[y,x] = np.nan  
    #Appending weighted average sequential changes into the ouput table
        dc = data.append(pd.Series(data.apply(lambda x:\
                                              np.average(x,weights = self.get_weight(self.yrlag))\
                                             ),name = 'WtSeq'),ignore_index = False)
        return dc #Output table spits historical WoW for ALL 53 WKs
#Todo: Add in replacement method
#pyseq: Getting weekly volume by WKNum for all past years
#Iterate over every cell to calculate sequential changes
    def py_seq(self, data):
        scenarios, adj_type = self.seq_scenarios, self.seq_adjtype
        #Part 1: Get all values 2)Get rid of multilayer 3)sort by WKNUM and 4)Drop index
        pastyearweeks = data[data['Year'].isin(self.sampleyr)].groupby(['Year','WKNum'])\
                                        .sum()['Calls'].reset_index()\
                                        .sort_values(by=['WKNum','Year'])\
                                        .reset_index(drop = True)
        pyseqpv = pastyearweeks.pivot_table(index = 'Year', columns = 'WKNum', values = 'Calls')\
                                        .rename_axis(None)#Pivot to get to correct layout
        histseq = self.calculate_seq(pyseqpv).iloc[-1,(self.startwk-1):].to_frame().transpose()
        
        #If users have manual adjustments - if statements to go through scenarios
        if self.ctradj:
            #Part 1: Reading user inputted adjustments from same excel file
            adjraw = pd.read_excel(self.filepath, sheet_name = f'{self.subentity}WKAdj'\
                            ,index_col = 0, header = 0)
            adjraw.columns = adjraw.columns.astype('float64').astype('int64')
            inyear = adjraw.iloc[:,np.where(adjraw.columns == self.startyr)[0]] #Filter to cur year
            wks = [int(x) for x in inyear.iloc[0,:]] #Get all Wk Number headers(Row 1)
            #Part 2: Decide on how many scenarios and prepare adjustment DF
            if scenarios == 1:#If only one scenario
                adjs = [y for y in inyear.iloc[1,:]] #Get user values (Row 2)
                comb = pd.DataFrame({wks[a] : adjs[a] for a in range(len(wks))}, index = ['CtRAdj'])
            elif scenarios > 1: #If more than one scneario
                adjs = []
                for x in range(len(inyear.columns)): #Check # of scnearios provided. X+1 to get row number
                    adjs.append([y for y in inyear.iloc[1:,x]]) #Loop through all avail rows and append
                comb = pd.DataFrame({wks[a] : adjs[a] for a in range(len(wks))})
            #Part 3: Declare 1)Dict to store all final adj and 2)loop thru all requested wks
            alldict = {}
            for x in range(self.startwk,self.endwk+1):
                #Part 3.1: Decide on user adjustment method
                if any(comb.columns.isin([x])): #If found in the override table
                    if adj_type == 'all': #Scenario 1.1 - If replace all, add 1 to user val and replace
                        wkval = comb.iloc[:,np.where(comb.columns == x)[0][0]].apply(lambda x: 1+x).values
                        alldict.update({x:wkval})
                    elif adj_type == 'sup': #Scenario 1.2 - If supplement, multipy hist and add 1 
                        wkhist = histseq.iloc[0:,np.where(histseq.columns == x)[0][0]].values[0]
                        wkval = comb.iloc[:,np.where(comb.columns == x)[0][0]]\
                                        .apply(lambda x: (1+x)*(wkhist)).values
                        alldict.update({x:wkval})
                elif any(histseq.columns.isin([x])): #If not found in override, use hist
                    alldict.update({x:histseq.iloc[0:,np.where(histseq.columns == x)[0][0]].values[0]})
                else: #If no value at all, insert NP NAN - rare case
                    alldict.update({x:np.nan})
            #Part 4: Convert into DataFrame
            finalseq = pd.DataFrame(alldict)
            return finalseq
        # If user didn't input overrides, return all historical sequential trends
        elif not self.ctradj:        
            return histseq
        #If User picks adj but no scenarios provided - Is this needed?
        else: 
            print(f'You chose adjustment but invalid scenarios number given - {scenarios}')
            sys.exit()
#Combining both calculated trailing average with historical weighted sequential changes
#Copy trailing avg table, append calculations and return forecast [1:] (excluding starting avg)
    def seq_forecast(self, weightedseq, trailingavgdf):
        if len(weightedseq.index) == 1: #If user only had one scenario
            trailingavg = trailingavgdf.copy()
            for x in range(len(weightedseq.columns)): #Length of weeks, get all weeks
                newwk = [] #Create a new list to store calculated values
                for y in range(trailingavg.shape[0]): #Iterates through every row of actual
                    newwk.append(trailingavg.iloc[y,-1] * weightedseq.iloc[0,x])
                    #print(cyact.iloc[y,-1],(1 + htseq.iloc[0,x]))
                     #use wk lag actuals * seq changes
                trailingavg[f'{weightedseq.columns[x]}'] = pd.Series(newwk, index = trailingavg.index)
            trailingavg = trailingavg.iloc[:,1:].apply(lambda x: x.astype('int64'))
            return trailingavg
        elif len(weightedseq.index) > 1: #If user had more than one scenario
            allscenarios = []
            for z in range(len(weightedseq.index)):
                trailingavg = trailingavgdf.copy()
                for x in range(len(weightedseq.columns)): #Length of weeks, get all weeks
                    newwk = [] #Create a new list to store calculated values
                    for y in range(trailingavg.shape[0]): #Iterates through every row of actual
                        newwk.append(trailingavg.iloc[y,-1] * weightedseq.iloc[z,x])    
                    trailingavg[f'{weightedseq.columns[x]}'] = pd.Series(newwk, index = trailingavg.index)
                trailingavg = trailingavg.iloc[:,1:].apply(lambda x: x.astype('int64'))
                allscenarios.append(trailingavg)
            return allscenarios
        
#TODO 1: Add weight parameter - Solved
#TODO 2: EoY be able to recognize and start a new year
#TODO 3: Recognize the forecast output and format correctly for display
def run_forecast(startwk, startyr, subentity, wklag, yrlag, customweight = []
                , ctradj = False, seq_scenarios = 1, seq_adjtype = 'sup'):
    instan = seq_model(startwk = startwk, startyr = startyr, subentity = subentity
                       , wklag = wklag, yrlag = yrlag, weightlist = customweight
                      , ctradj = ctradj, seq_scenarios = seq_scenarios, seq_adjtype = seq_adjtype)
    hist = instan.fetch_history(preview = False)
    htseq = instan.py_seq(hist)
    cyact = instan.trailing_avg(hist)
    forecast = instan.seq_forecast(weightedseq = htseq, trailingavgdf = cyact)
    if isinstance(forecast, list):
        finalfcst= pd.concat([x for x in forecast], keys = [f'Case{x+1}' for x in range(len(forecast))])
    else:
        finalfcst = forecast
    return finalfcst
