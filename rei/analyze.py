# Import the model we are using
import pandas as pd
import numpy as np

# needed to create new pipeline functions
from sklearn.base import BaseEstimator, TransformerMixin

from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LassoCV
from sklearn import metrics, impute, pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

try:
    from . import sdat
except:
    import sdat

import joblib

class AddMedianPrice(BaseEstimator, TransformerMixin):
    def __init__(self, variables=list()):
    #Check if the variables passed are in a list format, if not convert 
    #to list format and assign it to self.variables to be used in later 
    #methods
        if not isinstance(variables,list):
            self.variables = [variables]
        else:
            self.variables = variables
    
    def fit(self, X:pd.DataFrame,y:pd.Series):
        # calculate median price
        df = pd.concat([X,y], axis=1)
        df.columns = list(X.columns) + ["y"]
        df = df.assign(sqft_rnd = round(X.sqft, -2))
        self.grps = self.variables + ["sqft_rnd"]
        
        self.df_medians = df.groupby(self.grps)["y"].median().reset_index().rename(columns={'y':"median_price"})
        return self
    
    def transform(self, X:pd.DataFrame):
        # assign sqft_rd
        X=X.copy()

        X = X.assign(sqft_rnd = round(X.sqft, -2)).merge(self.df_medians,how='left', on=self.grps, suffixes=("_x", ""))
        
        return X

class Clean(BaseEstimator, TransformerMixin):
    def __init__(self,cat_thresh=.05,n_cats_max=20, drop_cols=list()):
        self.cat_thresh = cat_thresh
        self.n_cats_max = n_cats_max
        if type(drop_cols) != list: drop_cols=[drop_cols]
        self.drop_cols = drop_cols
        
        

    def fit(self, X:pd.DataFrame, y:pd.Series=None):
        self.columns = set(X.columns.tolist())
        df = pd.concat([X,y], axis=1)
        df.columns = list(X.columns) + ["y"]
        self.cat_vars = X.select_dtypes(include='object').columns.tolist()
        self.num_vars = X.select_dtypes(include=np.number).columns.tolist()

        for var in self.cat_vars:
            if len(df[var].unique()) / len(df) > self.cat_thresh:
                df.drop(columns =var, inplace=True)
                
                self.drop_cols.append(var)

        self.cat_vars=set(self.cat_vars) - set(self.drop_cols)
        # dictionary to hold categorical variables and their unique categories
        self.cats_ = dict()
        
        for var in self.cat_vars:
            top_vals = df.groupby([var])["y"].sum().sort_values(
                ascending=False).reset_index()
            top_vals = top_vals.loc[range(0,min(self.n_cats_max+1, len(top_vals))),var].unique().tolist() + ["Other"]     
            self.cats_[var] = top_vals
            
        return self

    def transform(self,X:pd.DataFrame):
        X = X.copy()
        
        # Drop columns not a part of original data set
        extra_cols = set(X.columns.tolist()) - self.columns
        X.drop(columns=extra_cols, inplace=True)
        
        # Fill categorical values that are missing
        # Fill categorical values not in original dataset
        for feature in self.cat_vars:
            X[feature] = X[feature].fillna("Other")
            new_cat = ~X[feature].isin(self.cats_[feature])
            X.loc[new_cat,feature] = "Other"


        # fix!!!
        for var in self.cat_vars:
            for cat in self.cats_[var]:
                col_name = str(var) +"_" + str(cat)
                X[col_name] = X[var] == cat
            X.drop(columns=var, inplace=True)
        


        # Clean Numerical Data
        # Not implemented
        for var in self.num_vars:
            X[var] = pd.to_numeric(X[var], errors='coerce')
        
        # Drop Columns
        for col in self.drop_cols:
            try:
                X.drop(columns=col, inplace=True)
            except:
                pass

        return X

if __name__ == '__main__':
    w = sdat.where_comps(miles=20, land_use=["Residential (R)", "Town House (TH)"], year="2020.06")
    df = sdat.sdat_query(where=w)
    df = sdat.add_features(df)
    df = df.query('sqft >0').assign(quality = None)
    for meta,frame in df.assign(sqft=round(df.sqft, -2)).groupby(["land_use","zipcode","basement", "sqft"]):
        try:
            res = pd.qcut(frame.price, q=[0,.05, .45, .55, .95,1], labels=["bottom","below", "avg", "above", "top"])
        except:
            res = None
        df.loc[frame.index, 'quality'] = res
    ##    try:
    ##        res = frame.price.median()
    ##    except:
    ##        res = np.nan
    ##    df.loc[frame.index, 'median_price'] = res
    ##
    ##df.median_price = pd.to_numeric(df.median_price)

    data=df.copy()
    #data = df[["price","zipcode","land_use", "stories",'basement',"sqft", "acre", "age","sale_type","quality"]].copy()
    #df[["price","zipcode","land_use", "stories","sqft", "acre", "age","quality"]].copy() 

    X=data.drop(columns='price')  
    y=df.price

    mods = dict()
    #cat_vars = X.select_dtypes('object').columns.tolist()
    #num_vars = X.select_dtypes(np.number).drop(columns='price').columns.tolist()
    #data = data.dropna(subset=cat_vars)
    #data = data[data.sqft > 0]



    # Parameters of pipelines can be set using ‘__’ separated parameter names:
    param_grid = {
        'rf__bootstrap': [True],
        'rf__max_depth': [80, 100],
        'rf__max_features': [2, 3],
        'rf__min_samples_leaf': [5],
        #'rf__min_samples_split': [8, 10, 12],
        'rf__n_estimators': [100, 250, 500, 1000]
    }

    #num_pipe = ColumnTransformer([
    #    ('num_missing', impute.SimpleImputer(), X.select_dtypes(np.number).columns.tolist())
    #    ])

    preprocess = pipeline.Pipeline([
        ('addMedian', AddMedianPrice(variables=['land_use', 'zipcode', 'basement'])),
        ('clean',Clean(drop_cols=["sale_date", "sale_date2","zipcode", "land_area_unit", 'tax_land_value', 'tax_improvement', 'tax_preferential_land_value', 'tax_assessment', 'tax_grade'])),
        ('num_missing', impute.SimpleImputer())
        #('num_missing',num_pipe)
        ])

    mods["rf"] = pipeline.Pipeline([
        ('preprocess', preprocess),
        ('rf',RandomForestRegressor())
        ])

    mods["lasso"] = pipeline.Pipeline([
        ('preprocess', preprocess),
        ('lasso',LassoCV(cv=10,random_state=3))
        ])   

    search = GridSearchCV(estimator = pipe, param_grid = param_grid, 
                            cv = 5, n_jobs = -1, verbose = 2)

    search.fit(X, y)
    print("Best parameter (CV score=%0.3f):" % search.best_score_)
    print(search.best_params_)

                    
    for mod in mods:
        print(mod)
        search = mods[mod]
        search.fit(X, y)


        preds = search.predict(X)
        per_error = preds / y
        print(metrics.r2_score(y,preds))
        print(metrics.mean_absolute_error(y,preds))
        print(np.mean(preds / y))


    # My House Guess
        h = sdat.Home('1303 Alberta Dr')
        print("Prediction: ",search.predict(h.meta.assign(quality='above')))
