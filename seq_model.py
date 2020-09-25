import pandas as pd
import numpy as np
import datetime as dt
from tkinter import *

class seq_model:
    def __init__(self, startwk, endwk = 53, subentity = 'NA',filepath = '', startyr = dt.date.today().year
                ,yrlag = 3, wklag = 3):
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
        
#QC Function - 
    def qc(self):
        print(self.filepath, self.yrlag, self.wklag, self.startwk, self.startyr, self.samplewk)

#Weight Assignment - Currently hardcoded - V1 TODO: Dynamically allow user to pass in overrides
    def get_weight(self, lag):
        weightlist = {
            1 : [1],
            2 : [0.5,0.5],
            3 : [0.15, 0.35, 0.5],
            4 : [0.125,0.175,0.3,0.4],
            5 : [0.05,0.1,0.2,0.25,0.4]
        }
        uselist = weightlist[lag]
        return uselist
        
#Fetch data from Excel storing historical data (NA & UK separate tabs) and format for manipulation
#Need to use columns for astype to work - alternative is FOR LOOP
#Subentity: Either 'NA' or 'UK', both not supported currently, need LOOP
#ctradj (Optional) = known adjustments we are making to every week
    def fetch_history(self, preview = False):
        hs = pd.read_excel(self.filepath, sheet_name = f'{self.subentity}Historical'\
                           ,usecols = 'A:M', header = 0)
        hs.drop(hs[~hs['DoWNum'].isin([x for x in range(1,8)])].index, inplace = True)
        hs[['Date','EoM']] = hs[['Date','EoM']].apply(pd.to_datetime)
        hs.iloc[:,np.r_[1,2,4,5,9,10,11,12]] = hs.iloc[:,np.r_[1,2,4,5,9,10,11,12]].astype('int64')
        #hs[hs.columns[np.r_[1,2,4,5,9,10,11,12]]] = hs.iloc[:,np.r_[1,2,4,5,9,10,11,12]].astype('int64')                                                            
        #If need to review dataframes prior to running forecast, set to TRUE
        if preview: print(hs.dtypes, hs.head(5), hs.tail(5)) 
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
                    data.iloc[y,x] = data.iloc[y,x]/data1.iloc[y,x]-1
                else: #If invalid, set it to np.nan
                    data.iloc[y,x] = np.nan  
    #Appending weighted average sequential changes into the ouput table
        dc = data.append(pd.Series(data.apply(lambda x:\
                                              np.average(x,weights = self.get_weight(self.yrlag))\
                                             ),name = 'WtSeq'),ignore_index = False)
        return dc #Output table spits historical WoW for ALL 53 WKs
    
#pyseq: Getting weekly volume by WKNum for all past years
#Iterate over every cell to calculate sequential changes
    def py_seq(self, data, ctradj = False):
        pyseq = data[data['Year'].isin(self.sampleyr)].groupby(['Year','WKNum'])\
                                                .sum()['Calls'].reset_index()\
                                                .sort_values(by=['WKNum','Year'])\
                                                .reset_index(drop = True)
        pyseqpv = pyseq.pivot_table(index = 'Year', columns = 'WKNum', values = 'Calls')\
                                                                    .rename_axis(None)
        histseq = self.calculate_seq(pyseqpv).iloc[-1,(self.startwk-1):].to_frame().transpose()
        
        #If users are inputting manual adjustments - override on weeks with available values
        if ctradj:
            #Part 1: Reading user inputted adjustments from same excel file
            adjraw = pd.read_excel(self.filepath, sheet_name = f'{self.subentity}WKAdj'\
                            ,index_col = 0, header = 0)
            adjraw.columns = adjraw.columns.astype('float64').astype('int64')
            inyear = adjraw.iloc[:,np.where(adjraw.columns == self.startyr)[0]] #Filter to cur year
            wks = [int(x) for x in inyear.iloc[0,:]] #Retrieve all Wk Number headers
            adjs = [y for y in inyear.iloc[1,:]] #Retrieve all corresponding values under WK Num
            comb = pd.DataFrame({wks[a] : adjs[a] for a in range(len(wks))}, index = ['CtRAdj'])#\
                #.iloc[:,wks.index(self.startwk):len(wks)]
           #Part 2: Combining historical and user input seq overrides based on priority 
            alldict = {}
            for x in range(self.startwk,self.endwk):
                if any(comb.columns.isin([x])): #If found in the override table
                    alldict.update({x:comb.iloc[0:\
                                                ,np.where(comb.columns == x)[0][0]].values[0]})
                elif any(histseq.columns.isin([x])): # Look for it in the historical seq table
                    alldict.update({x:histseq.iloc[0:\
                                                ,np.where(histseq.columns == x)[0][0]].values[0]})
                else:
                    alldict.update({x:np.nan})
                finalseq = pd.DataFrame(alldict, index = ['CtRAdj'])
            return finalseq
        else:# If user didn't input overrides, return all historical sequential trends
            return histseq


#Combining both calculated trailing average with historical weighted sequential changes
#Copy trailing avg table, append calculations and return forecast [1:] (excluding starting avg)
    def seq_forecast(self, weightedseq, trailingavgdf):
        trailingavg = trailingavgdf.copy()
        for x in range(len(weightedseq.columns)): #Check the length of weeks need to forecasted
            newwk = [] #Create a new list to store calculated values
            for y in range(trailingavg.shape[0]): #Iterates through every row of actual
                #print(cyact.iloc[y,-1],(1 + htseq.iloc[0,x]))
                newwk.append(trailingavg.iloc[y,-1] * (1 + weightedseq.iloc[0,x])) #use wk lag actuals * seq changes
            trailingavg[f'{weightedseq.columns[x]}'] = pd.Series(newwk, index = trailingavg.index)
        trailingavg = trailingavg.iloc[:,1:].apply(lambda x: x.astype('int64'))

        return trailingavg
    
def run_forecast(startwk, subentity, startyr, wklag, yrlag):
    instan = seq_model(startwk = startwk, subentity = subentity, 
                       startyr = startyr, wklag = wklag, yrlag = yrlag)
    hist = instan.fetch_history(preview = False)
    htseq = instan.py_seq(hist)
    cyact = instan.trailing_avg(hist)
    forecast = instan.seq_forecast(weightedseq = htseq, trailingavgdf = cyact)
    return forecast
