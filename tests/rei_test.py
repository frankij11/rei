#%%
import hvplot.pandas
from rei import sdat
import statsmodels.formula.api as smf
#%%

#%%
#df = sdat.sdat_query(where=sdat.where_comps(miles=1, year=0))
H= sdat.Home('1303 Alberta Dr')
H.ols()

#%%

meta = H.meta
comps = H.comps
# %%
comps.hvplot(x='sqft', y='price', kind='scatter', color='basement')
# %%
H.comps_filtered
# %%
filt=comps.query(f"""(sqft > {meta.sqft[0] * .8} & sqft < {H.meta.sqft[0] * 1.2}) & (basement == {meta.basement[0]} ) & (isBank ==False | flip==True | isCompany==True)""")[['address', 'sqft', 'price', 'sale_date', 'dist_kj']]
filt.sort_values('price')
# %%
