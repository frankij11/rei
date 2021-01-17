#%%
from rei import sdat
print("Hello world")
#%%

#%%
df = sdat.sdat_query(where=sdat.where_comps(miles=1, year=0))
H= sdat.Home('1303 Alberta Dr')
print("Hello world")
#%%

meta = H.meta
comps = H.comps
# %%
