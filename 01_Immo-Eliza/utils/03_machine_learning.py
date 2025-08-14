import pandas as pd
import numpy as np
import warnings, re, time
from os import path
from pandas.errors import DtypeWarning
#from pycaret.regression import *
from lightgbm import LGBMRegressor
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from sklearn.metrics import make_scorer, mean_absolute_error
from sklearn.model_selection import cross_val_score, KFold
#from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.impute import KNNImputer

warnings.simplefilter(action='ignore', category=DtypeWarning)

df = pd.read_csv(path.join(path.dirname(__file__), '..', 'data', 'cleaned_data.csv'))

# =========================================================================================
#                                  GLOBAL FUNCTION                                        
# =========================================================================================

"""
def remove_outliers_iqr(df, columns):
    df_cleaned = df.copy()
    for col in columns:
        Q1 = df_cleaned[col].quantile(0.25)
        Q3 = df_cleaned[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df_cleaned = df_cleaned[(df_cleaned[col] >= lower_bound) & (df_cleaned[col] <= upper_bound)]
    return df_cleaned

def choose_model(df):
    setup(data=df,target='transaction_sale_price',session_id=123,normalize=True,remove_multicollinearity=True,
          ignore_features=['id', 'postal_code', 'Unnamed: 0'])
    compare_models(n_select=5, sort='MAE')
"""

def sanitize_column_names(df):
    sanitized_columns = [re.sub(r'[{}[\]"\'\\:;<>.,?/|`~!@#$%^&*()=+]', '_', col) for col in df.columns]
    df.columns = sanitized_columns
    return df

df = pd.get_dummies(df, columns=df.select_dtypes(include=['object', 'category']).columns.tolist())
df = sanitize_column_names(df)

# =========================================================================================
#                                  MODEL LIGHTGBM                                         
# =========================================================================================

def choose_columns_lightgbm(df):
    df = df #.sample(frac=0.5, random_state=42)
    setup(data=df,target='transaction_sale_price',fold=5,session_id=42,use_gpu=True,verbose=False,ignore_features=['id', 'postal_code', 'Unnamed: 0'])
    lgbm = create_model('lightgbm', device='gpu', n_jobs=1, verbose=False)
    tuned_lgbm = tune_model(lgbm,custom_grid = {'num_leaves': [31, 50, 70],'max_depth': [5, 7],'min_data_in_leaf': [20, 30],'feature_fraction': [0.7, 0.9],
                                                'bagging_fraction': [0.7, 0.9],'max_bin': [64, 128]},fold=5,optimize='MAE',n_iter=4,early_stopping=True,verbose=False)
    final_model = finalize_model(tuned_lgbm)
    importances = final_model.feature_importances_
    features = tuned_lgbm.feature_name_
    feat_imp_df = pd.DataFrame({'feature': features, 'importance': importances}).sort_values(by='importance', ascending=False)
    return feat_imp_df['feature'].to_list()

def train_lightgbm_model(df, cv=5):
    X = df.drop(columns=['transaction_sale_price'])
    y = df['transaction_sale_price']
    mae_scorer = make_scorer(mean_absolute_error, greater_is_better=False)
    lgbm = LGBMRegressor(n_estimators=40, random_state=42, n_jobs=-1, verbose=-1)
    scores = cross_val_score(lgbm, X, y, scoring=mae_scorer, cv=cv, n_jobs=-1)*-1
    print(f"LGBMRegressor (cv={cv}):         {scores.mean():.2f} — {df.shape}")

if False:
    cols = ['transaction_sale_price', 'property_netHabitableSurface', 'property_bathroomCount']

    cat_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
    df = pd.get_dummies(df, columns=cat_columns)

    order = ['transaction_sale_price'] + choose_columns_lightgbm(df)
        
    df = remove_outliers_iqr(df, cols)
    df = sanitize_column_names(df)

    print(df.shape)
    for o in range(5,25):
        small_df = df[:o]
        train_lightgbm_model(small_df,5)
        print("\n","="*20,"\n")


    input("fini ...")

# =========================================================================================
#                                  MODEL RANDOMFOREST                                     
# =========================================================================================

def choose_columns_rf(df):
    df = df.copy()
    y = df['transaction_sale_price']
    X = df.drop(columns=['transaction_sale_price', 'id', 'postal_code', 'Unnamed_ 0'], errors='ignore')
    model = RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42, n_jobs=-1)
    model.fit(X, y)
    importances = model.feature_importances_
    feat_imp_df = pd.DataFrame({'feature': X.columns, 'importance': importances})
    feat_imp_df = feat_imp_df.sort_values(by='importance', ascending=False)
    return feat_imp_df['feature'].to_list()

def nan_management(df, column_choose):
    cols_mode    = ['property_bathroomCount', 'property_netHabitableSurface', 'property_basement_surface']
    cols_mean    = ['property_building_constructionYear']
    cols_fill_0  = ['property_building_floorCount']
    cols_drop_na = []
    cols_knni    = []
    df[cols_mode] = df[cols_mode].apply(lambda col: col.replace(-1, np.nan).fillna(col[col != -1].mode()[0]))
    df[cols_mean] = df[cols_mean].replace(-1, np.nan)
    df[cols_mean] = df[cols_mean].fillna(df[cols_mean].mean())
    df[cols_fill_0] = df[cols_fill_0].replace(-1, np.nan).fillna(0)
    for col in cols_drop_na:
        df = df[df[col] != -1]
    df = df.dropna(subset=cols_drop_na)
    if cols_knni:
        from sklearn.impute import KNNImputer
        imputer = KNNImputer(n_neighbors=5)
        df[cols_knni] = df[cols_knni].replace(-1, np.nan)
        df[cols_knni] = pd.DataFrame(imputer.fit_transform(df[cols_knni]), columns=cols_knni, index=df.index)

    nan_count = (df[column_choose] == -1).sum()
    print(f"NaN (-1) in '{column_choose}' : {nan_count}")

    #MAE = train_rf_model(df, cv=5)
    #print(f"RandomForestRegressor on '{column_choose}' : {MAE:.2f} — {df.shape} => fill_-1")

    strategies = [('fill_0', 0), ('mean', 'mean'), ('mode', 'most_frequent'), ('knn', 'knn')]

    for name, strategy in strategies:
        df_temp = df.copy()
        
        if name == 'fill_0':
            df_temp.loc[df_temp[column_choose] == -1, column_choose] = 0
            MAE = train_rf_model(df_temp, cv=5)

        elif name == 'knn' and nan_count < 28000 :
            df_temp[column_choose] = df_temp[column_choose].replace(-1, np.nan)
            num_cols = df_temp.select_dtypes(include='number').columns.tolist()
            num_cols.remove('transaction_sale_price')
            X_temp = df_temp[num_cols]
            y_temp = df_temp['transaction_sale_price']
            imputer = KNNImputer(n_neighbors=3)
            X_temp_imputed = imputer.fit_transform(X_temp)
            X_temp_imputed = pd.DataFrame(X_temp_imputed, columns=num_cols)
            rf = RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=2)
            mae_scorer = make_scorer(mean_absolute_error, greater_is_better=False)
            scores = cross_val_score(rf, X_temp_imputed, y_temp, cv=3, scoring=mae_scorer, n_jobs=2) * -1
            MAE = scores.mean()
        elif name != 'knn':
            df_temp.loc[df_temp[column_choose] == -1, column_choose] = np.nan
            MAE = train_rf_model(df_temp, cv=5, col_to_impute=column_choose, impute_strategy=strategy)
        
        print(f"RandomForestRegressor on '{column_choose}' : {MAE:.2f} — {df.shape} => {name}")

    if nan_count < 10000:
        df = df[df[column_choose] != -1]
        MAE = train_rf_model(df, cv=5)
        print(f"RandomForestRegressor on '{column_choose}' : {MAE:.2f} — {df.shape} => delete NaN")

def evaluate_outlier_strategies(df, column):
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    y = df['transaction_sale_price']
    X = df.drop(columns=['transaction_sale_price'])

    results = {}
    dfs = {}

    # NONE — on ne touche à rien
    X_none = X.copy()
    y_none = y.copy()
    pipeline_none = Pipeline([
        ('model', RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=1))
    ])
    scores_none = cross_val_score(pipeline_none, X_none, y_none, cv=kf,
                                  scoring=make_scorer(mean_absolute_error, greater_is_better=False), n_jobs=1)
    mae_none = -scores_none.mean()
    results['none'] = mae_none
    dfs['none'] = df.copy()
    print(f"NONE — MAE: {mae_none:.2f}")

    # CAPPING
    X_capping = X.copy()
    y_capping = y.copy()
    X_capping[column] = X_capping[column].replace(-1, np.nan)
    lower_cap = X_capping[column].quantile(0.01)
    upper_cap = X_capping[column].quantile(0.99)
    X_capping[column] = X_capping[column].clip(lower=lower_cap, upper=upper_cap)
    preprocessor_capping = ColumnTransformer([
        ('imputer', SimpleImputer(strategy='constant', fill_value=-1), [column])
    ], remainder='passthrough')
    pipeline_capping = Pipeline([
        ('preprocessor', preprocessor_capping),
        ('model', RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=1))
    ])
    scores_capping = cross_val_score(pipeline_capping, X_capping, y_capping, cv=kf,
                                     scoring=make_scorer(mean_absolute_error, greater_is_better=False), n_jobs=1)
    mae_capping = -scores_capping.mean()
    df_capping = df.copy()
    df_capping[column] = X_capping[column]
    results['capping'] = mae_capping
    dfs['capping'] = df_capping
    print(f"CAPPING — MAE: {mae_capping:.2f}")

    # LOG
    X_log = X.copy()
    y_log = y.copy()
    X_log[column] = X_log[column].replace(-1, np.nan)
    X_log[column] = np.log1p(X_log[column])
    preprocessor_log = ColumnTransformer([
        ('imputer', SimpleImputer(strategy='constant', fill_value=-1), [column])
    ], remainder='passthrough')
    pipeline_log = Pipeline([
        ('preprocessor', preprocessor_log),
        ('model', RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=1))
    ])
    scores_log = cross_val_score(pipeline_log, X_log, y_log, cv=kf,
                                 scoring=make_scorer(mean_absolute_error, greater_is_better=False), n_jobs=1)
    mae_log = -scores_log.mean()
    df_log = df.copy()
    df_log[column] = X_log[column]
    results['log'] = mae_log
    dfs['log'] = df_log
    print(f"LOG — MAE: {mae_log:.2f}")

    # DELETE
    X_delete = X.copy()
    y_delete = y.copy()
    X_delete[column] = X_delete[column].replace(-1, np.nan)
    q1, q3 = X_delete[column].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    mask = (X_delete[column] >= lower_bound) & (X_delete[column] <= upper_bound)
    X_delete = X_delete.loc[mask]
    y_delete = y_delete.loc[mask]
    df_delete = df.loc[mask]

    preprocessor_delete = ColumnTransformer([
        ('imputer', SimpleImputer(strategy='constant', fill_value=-1), [column])
    ], remainder='passthrough')
    pipeline_delete = Pipeline([
        ('preprocessor', preprocessor_delete),
        ('model', RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=1))
    ])
    scores_delete = cross_val_score(pipeline_delete, X_delete, y_delete, cv=kf,
                                    scoring=make_scorer(mean_absolute_error, greater_is_better=False), n_jobs=1)
    mae_delete = -scores_delete.mean()
    results['delete'] = mae_delete
    dfs['delete'] = df_delete
    print(f"DELETE — MAE: {mae_delete:.2f} (rows left: {len(X_delete)})")

    # Choix de la meilleure méthode
    non_delete_methods = {k: v for k, v in results.items() if k != 'delete'}
    best_non_delete_method = min(non_delete_methods, key=non_delete_methods.get)
    best_non_delete_mae = results[best_non_delete_method]
    best_non_delete_rows = len(df)

    rows_after_delete = len(df_delete)
    row_loss = best_non_delete_rows - rows_after_delete
    mae_gain = best_non_delete_mae - mae_delete
    ratio = mae_gain / row_loss if row_loss > 0 else 0

    print(f"\n🔍 MAE gain: {mae_gain:.2f} | Row loss: {row_loss} | Ratio: {ratio:.3f}")

    if ratio > 1:
        print(f"✅ Méthode sélectionnée : DELETE")
        return df_delete, mae_delete
    else:
        print(f"✅ Méthode sélectionnée : {best_non_delete_method.upper()}")
        return dfs[best_non_delete_method], results[best_non_delete_method]


if True:
    #order = ['transaction_sale_price'] + choose_columns_rf(df)
    order = ['transaction_sale_price', 'property_bathroomCount', 'property_location_latitude', 'property_netHabitableSurface', 'property_location_postalCode', 'property_location_longitude', 'transaction_certificates_epcScore', 'property_bedroomCount', 'transaction_sale_cadastralIncome', 'property_basement_surface', 'property_building_constructionYear', 'property_hasSecureAccessAlarm', 'property_subtype_house', 'property_location_floor', 'property_subtype_villa', 'property_hasCaretakerOrConcierge', 'property_location_hasSeaView', 'property_toiletCount', 'property_parkingCountOutdoor', 'property_subtype_apartment_block', 'property_showerRoomCount', 'transaction_certificates_primaryEnergyConsumptionPerSqm', 'property_building_floorCount', 'property_terraceSurface', 'transaction_certificates_carbonEmission', 'property_hasAirConditioning', 'property_roomCount', 'property_subtype_apartment', 'property_building_condition', 'isHouse', 'property_hasSwimmingPool', 'property_livingRoom_surface', 'property_specificities_hasOffice', 'property_parkingCountIndoor', 'property_specificities_hasWorkspace', 'terrace_cos', 'property_hasArmoredDoor', 'property_building_facadeCount', 'property_hasLaundryRoom', 'property_subtype_mixed_use_building', 'property_kitchen_type_hyper_equipped', 'property_hasLift', 'property_energy_hasHeatPump', 'property_location_type_urban', 'property_kitchen_surface', 'property_hasCableTV', 'property_hasDisabledAccess', 'property_constructionPermit_constructionType_all_kind', 'property_subtype_castle', 'property_gardenSurface', 'property_propertyCertificates_hasAsbestosCertificate', 'property_subtype_ground_floor', 'property_subtype_exceptional_property', 'property_hasDressingRoom', 'property_hasVisiophone', 'property_hasBasement', 'property_land_iswooded', 'property_hasSauna', 'property_energy_hasThermicPanels', 'transaction_certificates_renovationObligation', 'property_hasDiningRoom', 'property_location_type_fitted_out', 'transaction_certificates_primaryEnergyConsumptionYearly', 'property_hasAttic', 'transaction_sale_isFurnished', 'property_propertyCertificates_primaryEnergyConsumptionLevel', 'property_hasDoorPhone', 'property_energy_hasPhotovoltaicPanels', 'property_constructionPermit_hasPlotDivisionAuthorization', 'property_energy_hasDoubleGlazing', 'property_energy_heatingType_gas', 'property_constructionPermit_hasObligationToConstruct', 'flags_isNewClassified', 'property_location_type_residential', 'property_constructionPermit_pScore', 'property_propertyCertificates_builtPlanStatus', 'property_constructionPermit_isObtained', 'property_constructionPermit_gScore', 'property_hasTerrace', 'property_propertyCertificates_hasElectricalInstallationComplianceCertificate', 'property_hasJacuzzi', 'property_energy_heatingType_fueloil', 'property_kitchen_type_installed', 'property_constructionPermit_isBreachingUrbanPlanningRegulation', 'property_energy_heatingType_none', 'property_hasLivingRoom', 'property_constructionPermit_hasPossiblePriorityPurchaseRight', 'property_location_type_none', 'property_land_hasgaswaterelectricityconnection', 'property_energy_hasCollectiveWaterHeater', 'property_constructionPermit_constructionType_bel_etage', 'property_constructionPermit_constructionType_bungalow', 'property_constructionPermit_constructionType_apartment_building', 'flags_isLifeAnnuitySale', 'property_constructionPermit_constructionType_house', 'flags_isAnInteractiveSale', 'property_constructionPermit_constructionType_none', 'property_location_type_shop_street', 'property_constructionPermit_constructionType_villa', 'property_location_type_not_fitted_out', 'property_location_type_mall', 'property_energy_heatingType_carbon', 'property_energy_heatingType_pellet', 'property_energy_heatingType_electric', 'property_kitchen_type_usa_hyper_equipped', 'property_land_sewerconnection_connected', 'property_land_sewerconnection_cannot_be_connected', 'property_land_sewerconnection_can_be_connected', 'property_kitchen_type_usa_uninstalled', 'property_kitchen_type_usa_semi_equipped', 'property_kitchen_type_usa_installed', 'property_kitchen_type_semi_equipped', 'flags_isNotarySale', 'property_kitchen_type_not_installed', 'property_kitchen_type_none', 'flags_isNewlyBuilt', 'property_energy_heatingType_wood', 'property_energy_heatingType_solar', 'property_location_type_isolated', 'property_location_type_landscape', 'property_subtype_triplex', 'flags_isUnderOption', 'property_land_surface', 'property_subtype_bungalow', 'garden_cos', 'property_attic_surface', 'property_attic_isisolated', 'property_specificities_office_surface', 'property_specificities_workspace_surface', 'property_kitchen_hasOven', 'property_subtype_country_cottage', 'property_land_isflat', 'property_land_isfacingstreet', 'property_land_hasplottorear', 'flags_isNewPrice', 'property_hasGarden', 'property_hasInternet', 'property_subtype_chalet', 'property_subtype_duplex', 'property_location_type_country', 'property_subtype_pavilion', 'property_location_type_concrete', 'property_location_type_compartmentalized', 'property_building_annexCount', 'property_subtype_town_house', 'property_subtype_service_flat', 'property_subtype_penthouse', 'property_subtype_other_property', 'property_subtype_farmhouse', 'property_subtype_mansion', 'property_subtype_manor_house', 'property_subtype_loft', 'property_subtype_kot', 'property_propertyCertificates_oilTankCertificateStatus', 'property_subtype_flat_studio', 'property_land_sewerconnection_not_connected'] 

def clean_outliers(df, col, cols_capping, cols_log, cols_delete):
    df = df.copy()
    df[col] = df[col].replace(-1, np.nan)
    if col in cols_capping:
        lower = df[col].quantile(0.01)
        upper = df[col].quantile(0.99)
        df[col] = df[col].clip(lower=lower, upper=upper)
    if col in cols_log:
        df[col] = np.log1p(df[col])
    if col in cols_delete:
        notna_mask = df[col].notna()
        col_valid = df.loc[notna_mask, col]
        q1 = col_valid.quantile(0.25)
        q3 = col_valid.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        keep_mask = ((df[col] >= lower) & (df[col] <= upper)) | df[col].isna()
        df = df[keep_mask]
    df[col] = df[col].fillna(-1)
    return df

cols_capping = []
cols_log     = []
cols_delete  = ['transaction_sale_price']
for col in order:
    if col in cols_capping or col in cols_log or col in cols_delete:
        df = clean_outliers(df, col, cols_capping, cols_log, cols_delete)
start = time.time()


df = df[:10000]
print(df.shape)
for i in range(1,10):
    df, best_MAE = evaluate_outlier_strategies(df, order[i])
    print("="*30, best_MAE)

"""
last_MAE = 1000000
column_choose = order[0]

for column_choose in order[1:]:
    if df[column_choose].dtype != 'bool':
        if '_has' not in column_choose and '_is' not in column_choose:
            if not df[column_choose].dropna().isin([-1,1,0]).all():
                if column_choose not in ('property_location_latitude','property_location_postalCode', 'property_location_longitude'):
                    df, last_MAE = outlier_management(df, column_choose, last_MAE)

"""


#cols = ['transaction_sale_price', 'property_netHabitableSurface', 'property_bathroomCount']
#df = remove_outliers_iqr(df, cols)

end = time.time()
print(f"Temps d'exécution : {end - start:.2f} secondes")

exit()

# =========================================================================================
#                                  MODEL EXTRATREES                                       
# =========================================================================================

def train_et_model(df, cv=5):
    X = df.drop(columns=['transaction_sale_price'])
    y = df['transaction_sale_price']
    mae_scorer = make_scorer(mean_absolute_error, greater_is_better=False)
    et = ExtraTreesRegressor(n_estimators=40, random_state=42, n_jobs=-1)
    scores = cross_val_score(et, X, y, scoring=mae_scorer, cv=cv, n_jobs=-1)*-1
    print(f"ExtraTreesRegressor (cv={cv}):   {scores.mean():.2f} — {df.shape}")

for o in range(10,20):
    small_df = df[:o]
    train_et_model(small_df,5)
    print("\n","="*20,"\n")


""" TEST POUR AFFICHER LES DIFFERENCES 2 MODELS
#order = ['transaction_sale_price'] + choose_columns_lightgbm(df)
#order = ['transaction_sale_price'] + choose_columns_rf(df)
order = ['transaction_sale_price'] + ['property_netHabitableSurface', 'property_location_latitude', 'property_location_longitude', 'property_location_postalCode', 'property_bathroomCount', 'transaction_sale_cadastralIncome', 'property_bedroomCount', 'property_building_constructionYear', 'transaction_certificates_primaryEnergyConsumptionPerSqm', 'transaction_certificates_epcScore', 'property_location_floor', 'property_toiletCount', 'property_terraceSurface', 'property_building_condition', 'property_building_floorCount', 'property_building_facadeCount', 'property_parkingCountIndoor', 'property_parkingCountOutdoor', 'property_gardenSurface', 'property_type_isHouse"', 'property_hasSecureAccessAlarm', 'property_showerRoomCount', 'property_hasVisiophone', 'property_subtype_apartment_block', 'property_subtype_villa', 'property_kitchen_surface', 'property_hasArmoredDoor', 'property_hasSwimmingPool', 'property_roomCount', 'property_propertyCertificates_primaryEnergyConsumptionLevel', 'property_hasLift', 'property_constructionPermit_pScore', 'property_hasLivingRoom', 'property_subtype_house', 'property_energy_hasHeatPump', 'property_energy_hasDoubleGlazing', 'property_constructionPermit_gScore', 'property_hasCableTV', 'property_kitchen_type_hyper_equipped', 'transaction_certificates_primaryEnergyConsumptionYearly', 'transaction_certificates_renovationObligation', 'property_subtype_exceptional_property', 'property_location_hasSeaView', 'property_energy_heatingType_none', 'property_specificities_hasOffice', 'property_hasDisabledAccess', 'property_hasDoorPhone', 'property_subtype_castle', 'property_land_surface', 'property_propertyCertificates_hasElectricalInstallationComplianceCertificate', 'flags_isUnderOption', 'property_subtype_farmhouse', 'flags_isNewlyBuilt', 'transaction_sale_isFurnished', 'property_subtype_mixed_use_building', 'property_constructionPermit_isObtained', 'property_energy_heatingType_gas', 'property_hasInternet', 'property_subtype_country_cottage', 'property_subtype_mansion', 'property_energy_hasPhotovoltaicPanels', 'flags_isNewPrice', 'property_subtype_penthouse', 'property_constructionPermit_hasObligationToConstruct', 'property_hasDiningRoom', 'property_basement_surface', 'property_hasTerrace', 'property_hasDressingRoom', 'property_hasGarden', 'property_hasAirConditioning', 'property_constructionPermit_hasPossiblePriorityPurchaseRight', 'property_specificities_hasWorkspace', 'property_land_hasgaswaterelectricityconnection', 'property_propertyCertificates_hasAsbestosCertificate', 'property_subtype_flat_studio', 'property_kitchen_type_installed', 'property_subtype_apartment', 'property_livingRoom_surface', 'property_location_type_none', 'property_hasCaretakerOrConcierge', 'property_hasBasement', 'property_location_type_urban', 'property_constructionPermit_constructionType_all_kind', 'property_subtype_kot', 'property_location_type_fitted_out', 'terrace_cos', 'property_constructionPermit_hasPlotDivisionAuthorization', 'property_subtype_manor_house', 'property_propertyCertificates_builtPlanStatus', 'property_location_type_landscape', 'flags_isNewClassified', 'property_hasLaundryRoom', 'property_constructionPermit_isBreachingUrbanPlanningRegulation', 'transaction_certificates_carbonEmission', 'property_kitchen_type_semi_equipped', 'property_energy_hasThermicPanels', 'property_location_type_residential', 'property_kitchen_type_not_installed', 'garden_cos', 'property_energy_hasCollectiveWaterHeater', 'property_subtype_duplex', 'property_location_type_country', 'property_constructionPermit_constructionType_none', 'property_energy_heatingType_wood', 'property_energy_heatingType_fueloil', 'property_constructionPermit_constructionType_villa', 'property_constructionPermit_constructionType_apartment_building', 'property_subtype_ground_floor', 'property_subtype_town_house', 'property_hasJacuzzi', 'property_location_type_isolated', 'property_hasAttic', 'property_building_annexCount', 'property_propertyCertificates_oilTankCertificateStatus', 'property_hasSauna', 'property_constructionPermit_constructionType_house', 'property_constructionPermit_constructionType_bel_etage', 'property_energy_heatingType_carbon', 'property_subtype_bungalow', 'property_land_hasplottorear', 'property_land_isfacingstreet', 'property_land_isflat', 'property_land_iswooded', 'property_land_sewerconnection_connected', 'property_land_sewerconnection_not_connected', 'property_land_sewerconnection_can_be_connected', 'property_land_sewerconnection_cannot_be_connected', 'flags_isAnInteractiveSale', 'property_specificities_workspace_surface', 'property_specificities_office_surface', 'flags_isLifeAnnuitySale', 'property_attic_isisolated', 'property_attic_surface', 'flags_isNotarySale', 'property_subtype_chalet', 'property_energy_heatingType_solar', 'property_energy_heatingType_pellet', 'property_location_type_compartmentalized', 'property_energy_heatingType_electric', 'property_kitchen_hasOven', 'property_kitchen_type_usa_semi_equipped', 'property_location_type_not_fitted_out', 'property_location_type_shop_street', 'property_location_type_concrete', 'property_location_type_mall', 'property_kitchen_type_usa_uninstalled', 'property_subtype_loft', 'property_kitchen_type_usa_hyper_equipped', 'property_kitchen_type_none', 'property_kitchen_type_usa_installed', 'property_subtype_pavilion', 'property_subtype_triplex', 'property_subtype_other_property', 'property_subtype_service_flat', 'property_constructionPermit_constructionType_bungalow']

order_2 = ['transaction_sale_price'] + choose_columns_rf(df)

for l in range(len(order)):
     print(f"{order[l]:<50} {order_2[l]}")
"""
