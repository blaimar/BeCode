import pandas as pd
import numpy as np
from os import path
import re, time, joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split, KFold

def load_df():
    def sanitize_column_names(df):
        sanitized_columns = [re.sub(r'[{}[\]"\'\\:;<>.,?/|~!@#$%^&*()=+]', '_', col) for col in df.columns]
        df.columns = sanitized_columns
        return df
    df = pd.read_csv(path.join(path.dirname(__file__), '..', 'data', 'cleaned_data.csv'))
    df = pd.get_dummies(df.replace({-1: np.nan, "-1": np.nan}), columns=df.select_dtypes(include=['object', 'category']).columns.tolist())
    return sanitize_column_names(df)

def choose_columns_rf(df, n_estimators=200, do_i_have_time = True):
    if do_i_have_time:
        df = df.replace({np.nan: -1})
        y = df['transaction_sale_price']
        X = df.drop(columns=['transaction_sale_price', 'id', 'Unnamed_ 0'], errors='ignore')
        model = RandomForestRegressor(n_estimators=n_estimators, max_depth=5, random_state=42, n_jobs=-1)
        model.fit(X, y)
        importances = model.feature_importances_
        feat_imp_df = pd.DataFrame({'feature': X.columns, 'importance': importances})
        feat_imp_df = feat_imp_df.sort_values(by='importance', ascending=False)
        order = feat_imp_df['feature'].to_list()
    else:
        order = ['property_netHabitableSurface', 'property_bathroomCount', 'property_bedroomCount', 'transaction_sale_cadastralIncome', 'transaction_certificates_primaryEnergyConsumptionPerSqm', 'transaction_certificates_epcScore', 'property_location_longitude', 'property_location_latitude', 'property_hasSwimmingPool', 'property_roomCount', 'property_subtype_villa', 'property_subtype_country_cottage', 'property_location_floor', 'property_building_annexCount', 'property_location_postalCode', 'property_building_constructionYear', 'property_subtype_house', 'property_building_facadeCount', 'property_hasSecureAccessAlarm', 'property_terraceSurface', 'property_building_condition', 'property_parkingCountIndoor', 'property_constructionPermit_constructionType_all_kind', 'property_hasLift', 'property_building_floorCount', 'property_hasDressingRoom', 'property_location_hasSeaView', 'property_constructionPermit_hasPlotDivisionAuthorization', 'property_toiletCount', 'property_gardenSurface', 'property_hasAirConditioning', 'property_subtype_castle', 'property_hasVisiophone', 'property_parkingCountOutdoor', 'property_subtype_apartment_block', 'property_energy_heatingType_fueloil', 'property_propertyCertificates_primaryEnergyConsumptionLevel', 'property_subtype_penthouse', 'transaction_sale_isFurnished', 'property_hasArmoredDoor', 'transaction_certificates_renovationObligation', 'property_constructionPermit_pScore', 'property_constructionPermit_isObtained', 'property_hasCableTV', 'flags_isUnderOption', 'property_hasDoorPhone', 'property_hasDisabledAccess', 'property_livingRoom_surface', 'property_propertyCertificates_hasElectricalInstallationComplianceCertificate', 'property_constructionPermit_hasObligationToConstruct', 'property_location_type_country', 'property_energy_hasDoubleGlazing', 'property_land_surface', 'property_land_isfacingstreet', 'property_propertyCertificates_builtPlanStatus', 'terrace_cos', 'property_hasAttic', 'property_hasInternet', 'property_location_type_fitted_out', 'property_showerRoomCount', 'property_land_isflat', 'property_land_iswooded', 'property_kitchen_type_usa_hyper_equipped', 'property_constructionPermit_gScore', 'property_subtype_exceptional_property', 'property_land_hasgaswaterelectricityconnection', 'property_specificities_office_surface', 'transaction_certificates_carbonEmission', 'property_subtype_mixed_use_building', 'property_land_sewerconnection_connected', 'property_type_isHouse', 'property_energy_hasHeatPump', 'property_energy_heatingType_gas', 'property_land_sewerconnection_not_connected', 'property_hasBasement', 'property_hasGarden', 'property_subtype_farmhouse', 'property_location_type_urban', 'property_hasJacuzzi', 'property_energy_hasThermicPanels', 'property_energy_hasPhotovoltaicPanels', 'property_hasSauna', 'property_land_hasplottorear', 'property_location_type_residential', 'property_propertyCertificates_hasAsbestosCertificate', 'property_hasCaretakerOrConcierge', 'property_basement_surface', 'property_specificities_workspace_surface', 'property_constructionPermit_isBreachingUrbanPlanningRegulation', 'property_kitchen_type_installed', 'property_specificities_hasOffice', 'property_kitchen_surface', 'transaction_certificates_primaryEnergyConsumptionYearly', 'property_constructionPermit_hasPossiblePriorityPurchaseRight', 'property_specificities_hasWorkspace', 'property_energy_hasCollectiveWaterHeater', 'property_constructionPermit_constructionType_villa', 'property_subtype_manor_house', 'property_hasTerrace', 'property_hasLaundryRoom', 'property_propertyCertificates_oilTankCertificateStatus', 'property_kitchen_type_not_installed', 'property_hasDiningRoom', 'property_constructionPermit_constructionType_apartment_building', 'property_hasLivingRoom', 'flags_isNewPrice', 'property_kitchen_type_hyper_equipped', 'property_location_type_landscape', 'flags_isNewClassified', 'property_subtype_other_property', 'property_energy_heatingType_electric', 'property_subtype_apartment', 'flags_isLifeAnnuitySale', 'flags_isAnInteractiveSale', 'flags_isNewlyBuilt', 'flags_isNotarySale', 'property_kitchen_hasOven', 'property_location_type_compartmentalized', 'property_subtype_triplex', 'property_subtype_town_house', 'property_subtype_service_flat', 'property_subtype_ground_floor', 'property_subtype_loft', 'property_subtype_mansion', 'property_subtype_flat_studio', 'property_subtype_kot', 'property_attic_isisolated', 'property_attic_surface', 'garden_cos', 'property_subtype_bungalow', 'property_subtype_chalet', 'property_subtype_duplex', 'property_location_type_concrete', 'property_constructionPermit_constructionType_house', 'property_constructionPermit_constructionType_bungalow', 'property_constructionPermit_constructionType_bel_etage', 'property_location_type_isolated', 'property_location_type_mall', 'property_location_type_not_fitted_out', 'property_location_type_shop_street', 'property_energy_heatingType_carbon', 'property_energy_heatingType_solar', 'property_energy_heatingType_pellet', 'property_energy_heatingType_wood', 'property_kitchen_type_usa_installed', 'property_kitchen_type_semi_equipped', 'property_land_sewerconnection_can_be_connected', 'property_kitchen_type_usa_semi_equipped']
    return order

def rf_mae(train, valid, target, featurs, n_estimators=100, random_state=42, n_jobs=2):
    model = RandomForestRegressor(n_estimators=n_estimators, random_state=random_state, n_jobs=n_jobs)
    model.fit(train[featurs], train[target])
    preds = model.predict(valid[featurs])
    return mean_absolute_error(valid[target], preds)

class Columns_sorter:
    def main(train_set, order, kf, do_i_have_time = True):
        if do_i_have_time:
            initial_columns = [col for col in order if col != 'transaction_sale_price']
            kept_columns = initial_columns.copy()
            throw_columns = []

            maes = []
            for train_idx, valid_idx in kf.split(train_set):
                train_fold = train_set.iloc[train_idx]
                valid_fold = train_set.iloc[valid_idx]
                mae = rf_mae(train_fold, valid_fold, 'transaction_sale_price', kept_columns, n_estimators=20, random_state=42, n_jobs=4)
                maes.append(mae)
                
            baseline_mae = np.mean(maes)
            print(f"🔧 Baseline MAE (all features): {baseline_mae:.2f}\n")

            for i, col in enumerate(initial_columns):
                test_columns = [c for c in kept_columns if c != col]
                maes = []

                for train_idx, valid_idx in kf.split(train_set):
                    train_fold = train_set.iloc[train_idx]
                    valid_fold = train_set.iloc[valid_idx]
                    mae = rf_mae(train_fold, valid_fold, 'transaction_sale_price', test_columns, n_estimators=20, random_state=42, n_jobs=4)
                    maes.append(mae)

                avg_mae = np.mean(maes)
                print(f"[{i+1:3}/{len(initial_columns)}] MAE: {avg_mae:9.2f} |", end='')

                if avg_mae <= baseline_mae - 100:
                    kept_columns.remove(col)
                    throw_columns.append(col)
                    baseline_mae = avg_mae
                    print(f" ❌ Removed {col}")
                else:
                    print(f" ✅ Kept {col}")

            print(f"\n✅ Final kept columns ({len(kept_columns)}):\n{kept_columns}")
            print(f"\n❌ Final removed columns ({len(throw_columns)}):\n{throw_columns}")

            kept_columns_ordered, throw_columns_ordered = [], []
            
            for col in order:
                if col in kept_columns:
                    kept_columns_ordered.append(col)
                elif col in throw_columns:
                    throw_columns_ordered.append(col)

        else:
            kept_columns_ordered = ['property_netHabitableSurface', 'property_bathroomCount', 'property_bedroomCount', 'transaction_sale_cadastralIncome', 'transaction_certificates_primaryEnergyConsumptionPerSqm', 'transaction_certificates_epcScore', 'property_location_longitude', 'property_roomCount', 'property_location_latitude', 'property_hasSwimmingPool', 'property_location_postalCode', 'property_location_floor', 'property_building_constructionYear', 'property_subtype_villa', 'property_building_annexCount', 'property_parkingCountIndoor', 'property_hasSecureAccessAlarm', 'property_subtype_country_cottage', 'property_building_facadeCount', 'property_terraceSurface', 'property_building_condition', 'property_constructionPermit_constructionType_all_kind', 'property_building_floorCount', 'property_hasDressingRoom', 'property_toiletCount', 'property_subtype_apartment_block', 'property_hasAirConditioning', 'property_constructionPermit_hasPlotDivisionAuthorization', 'property_location_hasSeaView', 'property_gardenSurface', 'transaction_sale_isFurnished', 'property_subtype_castle', 'transaction_certificates_renovationObligation', 'property_subtype_penthouse', 'property_constructionPermit_hasObligationToConstruct', 'property_hasDisabledAccess', 'property_hasArmoredDoor', 'property_hasVisiophone', 'property_hasDoorPhone', 'property_parkingCountOutdoor', 'property_energy_heatingType_fueloil', 'property_propertyCertificates_primaryEnergyConsumptionLevel', 'property_constructionPermit_isObtained', 'property_showerRoomCount', 'property_livingRoom_surface', 'property_specificities_office_surface', 'property_energy_hasDoubleGlazing', 'property_land_sewerconnection_not_connected', 'property_constructionPermit_pScore', 'property_type_isHouse', 'property_location_type_country', 'property_propertyCertificates_builtPlanStatus', 'property_energy_hasPhotovoltaicPanels', 'property_constructionPermit_isBreachingUrbanPlanningRegulation', 'property_hasSauna', 'property_land_isfacingstreet', 'property_land_hasgaswaterelectricityconnection', 'property_land_hasplottorear', 'property_constructionPermit_constructionType_villa', 'property_location_type_urban', 'property_subtype_farmhouse', 'property_kitchen_surface', 'property_hasCableTV', 'property_propertyCertificates_hasElectricalInstallationComplianceCertificate', 'property_land_isflat', 'property_location_type_residential', 'property_energy_hasCollectiveWaterHeater', 'property_kitchen_type_usa_hyper_equipped', 'property_land_iswooded', 'property_constructionPermit_gScore', 'property_subtype_manor_house', 'property_hasTerrace', 'property_land_sewerconnection_connected', 'terrace_cos', 'property_propertyCertificates_oilTankCertificateStatus', 'property_hasInternet', 'property_kitchen_type_installed', 'property_energy_heatingType_gas', 'property_hasCaretakerOrConcierge', 'property_subtype_mixed_use_building', 'property_energy_heatingType_electric', 'property_hasDiningRoom', 'property_location_type_mall', 'property_location_type_shop_street', 'property_location_type_landscape', 'property_location_type_fitted_out', 'property_location_type_not_fitted_out', 'property_location_type_concrete', 'property_location_type_isolated', 'flags_isNotarySale', 'property_constructionPermit_constructionType_apartment_building', 'property_hasGarden', 'property_constructionPermit_constructionType_bel_etage', 'property_constructionPermit_constructionType_bungalow', 'property_constructionPermit_constructionType_house', 'property_energy_heatingType_carbon', 'flags_isNewlyBuilt', 'property_energy_heatingType_pellet', 'flags_isLifeAnnuitySale', 'property_energy_heatingType_solar', 'property_energy_heatingType_wood', 'property_kitchen_type_hyper_equipped', 'property_kitchen_type_not_installed', 'property_kitchen_type_semi_equipped', 'property_kitchen_type_usa_installed', 'property_kitchen_type_usa_semi_equipped', 'property_land_sewerconnection_can_be_connected', 'property_location_type_compartmentalized', 'property_subtype_flat_studio', 'property_subtype_triplex', 'transaction_certificates_carbonEmission', 'property_attic_isisolated', 'property_hasBasement', 'property_specificities_workspace_surface', 'property_propertyCertificates_hasAsbestosCertificate', 'flags_isNewPrice', 'transaction_certificates_primaryEnergyConsumptionYearly', 'property_constructionPermit_hasPossiblePriorityPurchaseRight', 'garden_cos', 'property_hasJacuzzi', 'property_specificities_hasWorkspace', 'property_specificities_hasOffice', 'property_energy_hasThermicPanels', 'property_kitchen_hasOven', 'property_hasLaundryRoom', 'property_attic_surface', 'property_subtype_apartment', 'property_subtype_town_house', 'flags_isUnderOption', 'property_subtype_service_flat', 'flags_isAnInteractiveSale', 'property_subtype_other_property', 'property_subtype_mansion', 'property_subtype_loft', 'property_subtype_kot', 'property_subtype_ground_floor', 'property_basement_surface', 'property_hasLivingRoom', 'property_subtype_exceptional_property', 'property_subtype_duplex', 'property_subtype_chalet', 'property_hasAttic', 'property_subtype_bungalow', 'flags_isNewClassified']
            throw_columns_ordered = ['property_subtype_house', 'property_hasLift', 'property_land_surface', 'property_energy_hasHeatPump']
        return kept_columns_ordered, throw_columns_ordered

class Outliers:
    def apply_outlier_method(df, col, method):
        df = df.copy()
        if method == 'Capping':
            lower = df[col].quantile(0.01)
            upper = df[col].quantile(0.99)
            df[col] = np.where(df[col] < lower, lower, df[col])
            df[col] = np.where(df[col] > upper, upper, df[col])

        elif method == 'Log':
            if not (df[col] < 0).any():
                df[col] = np.log1p(df[col])

        elif method == 'Delete':
            q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            df = df[(df[col] >= lower) & (df[col] <= upper)]

        return df

    def evaluate_model(df_train, df_valid, features, target, n_estimators=100):
        model = RandomForestRegressor(n_estimators=n_estimators, random_state=42, n_jobs=2)
        model.fit(df_train[features], df_train[target])
        preds = model.predict(df_valid[features])
        mae = mean_absolute_error(df_valid[target], preds)
        return mae

    def outliers_management(df, col, mapping, min_lines=30_000, n_estimators=100):
        target = 'transaction_sale_price'
        features = df.columns.drop(target).tolist()

        kf = KFold(n_splits=5, shuffle=True, random_state=42)
        all_methods = {m: {'maes': [], 'rows': []} for m in ['None', 'Capping', 'Log', 'Delete']}
        for fold, (train_idx, valid_idx) in enumerate(kf.split(df)):
            train_fold = df.iloc[train_idx].copy()
            valid_fold = df.iloc[valid_idx].copy()

            # remplacer -1 par NaN dans les folds
            train_fold.replace(-1, np.nan, inplace=True)
            valid_fold.replace(-1, np.nan, inplace=True)

            # Appliquer les méthodes déjà décidées dans le mapping aux colonnes précédentes
            for prev_col, prev_method in mapping.items():
                if prev_col == col: continue
                train_fold = Outliers.apply_outlier_method(train_fold, prev_col, prev_method)
                valid_fold = Outliers.apply_outlier_method(valid_fold, prev_col, prev_method)

            # pour chaque méthode, appliquer et évaluer
            for method in all_methods.keys():
                transformed_train = train_fold.copy()
                transformed_valid = valid_fold.copy()

                if method == 'Delete':
                    q1 = transformed_train[col].quantile(0.25)
                    q3 = transformed_train[col].quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr

                    transformed_train = transformed_train[(transformed_train[col] >= lower) & (transformed_train[col] <= upper)]
                    transformed_valid = transformed_valid[(transformed_valid[col] >= lower) & (transformed_valid[col] <= upper)]

                elif method == 'Log':
                    if (transformed_train[col] < 0).any() or (transformed_valid[col] < 0).any():
                        print(f"col={col} fold={fold+1} method={method:7}: skip (negatives)")
                        all_methods[method]['maes'].append(np.inf)
                        all_methods[method]['rows'].append(0)
                        continue
                    transformed_train[col] = np.log1p(transformed_train[col])
                    transformed_valid[col] = np.log1p(transformed_valid[col])

                elif method == 'Capping':
                    lower = transformed_train[col].quantile(0.01)
                    upper = transformed_train[col].quantile(0.99)
                    transformed_train[col] = np.clip(transformed_train[col], lower, upper)
                    transformed_valid[col] = np.clip(transformed_valid[col], lower, upper)

                # Replace NaN with -1 before training
                transformed_train.replace(np.nan, -1, inplace=True)
                transformed_valid.replace(np.nan, -1, inplace=True)

                intersecting_features = list(set(features) & set(transformed_train.columns))
                try:
                    mae = Outliers.evaluate_model(transformed_train, transformed_valid, intersecting_features, target, n_estimators)
                    all_methods[method]['maes'].append(mae)
                    all_methods[method]['rows'].append(len(transformed_train))
                    print(f"col={col} fold={fold+1} method={method:7}: MAE={mae:9.2f}, Rows={len(transformed_train)}")
                except Exception as e:
                    print(f"col={col} fold={fold+1} method={method} failed: {e}")
                    all_methods[method]['maes'].append(np.inf)
                    all_methods[method]['rows'].append(0)

        # Résumé des scores (moyenne sur folds)
        summed = {
            method: {
                'mae': np.sum(results['maes']),
                'rows': np.sum(results['rows'])
            } for method, results in all_methods.items()
        }

        best_non_destructive = min(['None', 'Capping', 'Log'], key=lambda m: summed[m]['mae'])
        delete_mae = summed['Delete']['mae']
        delete_rows = summed['Delete']['rows']
        mae_diff = summed[best_non_destructive]['mae'] - delete_mae
        row_diff = summed[best_non_destructive]['rows'] - delete_rows
        ratio = mae_diff / (row_diff + 1e-9)  # éviter division par zéro

        print(f"\n{col} Summary:")
        for method in all_methods:
            print(f"{method:7}: MAE={(summed[method]['mae'] / 5 if summed[method]['mae'] != np.inf else summed[method]['mae']):6.2f}, Rows={summed[method]['rows']//5}")

        if ratio > 2 and delete_rows//5 >= min_lines:
            print(f"Method 'Delete' selected for {col} (ratio: {ratio:.2f})\n")
            mapping[col] = 'Delete'
        else:
            print(f"Delete method rejected: ratio too low ({ratio:.2f}) or too few rows ({delete_rows//5})")
            print(f"Method '{best_non_destructive}' selected for {col}\n")
            mapping[col] = best_non_destructive

        return mapping

    def main(train_set, test_set, order):
        features = train_set.columns.drop('transaction_sale_price').tolist()
        mae_base = Outliers.evaluate_model(train_set, test_set, features, 'transaction_sale_price', 100)

        mapping_outliers = {}
        i = 1
        for col in order:
            if train_set[col].dtype != 'bool':
                if '_has' not in col and '_is' not in col:
                    if not train_set[col].dropna().isin([-1, 0, 1]).all():
                        if col not in ('property_location_latitude', 'property_location_postalCode', 'property_location_longitude'):
                            print(i)
                            i += 1
                            mapping_outliers = Outliers.outliers_management(train_set, col, mapping_outliers, 40_000, 80)

        print(f"{mapping_outliers}")

        print(f"\nTrain size: {len(train_set)} | Test size: {len(test_set)} | Total size: {len(train_set)+len(test_set)}")
        print(f"✅ BASELINE MAE (no processing): {mae_base:.2f}")

        train_processed = train_set.copy()
        test_processed = test_set.copy()

        for col, method in mapping_outliers.items():
            train_processed = Outliers.apply_outlier_method(train_processed, col, method)
            test_processed = Outliers.apply_outlier_method(test_processed, col, method)

        features = train_set.columns.drop('transaction_sale_price').tolist()
        mae_final = Outliers.evaluate_model(train_processed, test_processed, features, 'transaction_sale_price', 100)
        print(f"\nTrain size: {len(train_processed)} | Test size: {len(test_processed)} | Total size: {len(train_processed)+len(test_processed)}")
        print(f"✅ FINAL MAE (after processing): {mae_final:.2f}")

        return mapping_outliers

class NaN:
    def apply_nan_method(df, col, method):
        df = df.copy()
        if method == 'None':
            df[col] = df[col].replace({-1:np.nan}).fillna(-1)
        elif method == 'Mean':
            df[col] = df[col].replace({-1:np.nan}).fillna(df[col].mean())
        elif method == 'Mode':
            df[col] = df[col].replace({-1:np.nan}).fillna(df[col].mode().iloc[0])
        elif method == 'Delete':
            # handled upstream (since deletion affects rows)
            pass
        return df

    def evaluate_model(df_train, df_valid, features, target, n_estimators=100):
        model = RandomForestRegressor(n_estimators=n_estimators, random_state=42, n_jobs=2)
        model.fit(df_train[features], df_train[target])
        preds = model.predict(df_valid[features])
        mae = mean_absolute_error(df_valid[target], preds)
        return mae

    def nan_management(df, col, mapping, min_lines=40_000, n_estimators=100):
        target = 'transaction_sale_price'
        features = df.columns.drop(target).tolist()
        methods = ['None', 'Mean', 'Mode', 'Delete']

        kf = KFold(n_splits=5, shuffle=True, random_state=42)
        all_methods = {m: {'maes': [], 'rows': []} for m in methods}

        for fold, (train_idx, valid_idx) in enumerate(kf.split(df)):
            train_fold = df.iloc[train_idx].copy()
            valid_fold = df.iloc[valid_idx].copy()

            # Appliquer les anciennes méthodes
            for prev_col, prev_method in mapping.items():
                if prev_col == col: continue
                train_fold[col] = train_fold[col].replace(-1, np.nan)
                valid_fold[col] = valid_fold[col].replace(-1, np.nan)
                train_fold = NaN.apply_nan_method(train_fold, prev_col, prev_method)
                valid_fold = NaN.apply_nan_method(valid_fold, prev_col, prev_method)

            train_fold[col] = train_fold[col].replace({-1:np.nan})
            count_nan = train_fold[col].isna().sum()
            print(f"{count_nan} NaN in {col} (fold {fold + 1}) :")
            if 0 < count_nan < len(train_fold[col]):
                for method in methods:
                    t_train = train_fold.copy()
                    t_valid = valid_fold.copy()

                    if method == 'Delete':
                        # Supprimer les lignes avec NaN uniquement sur la colonne courante
                        t_train = t_train[~t_train[col].isna()]
                        t_valid = t_valid[~t_valid[col].isna()]
                    else:
                        t_train = NaN.apply_nan_method(t_train, col, method)
                        t_valid = NaN.apply_nan_method(t_valid, col, method)

                    # Modèle
                    intersecting_features = list(set(features) & set(t_train.columns))
                    mae = NaN.evaluate_model(t_train, t_valid, intersecting_features, target, n_estimators)
                    all_methods[method]['maes'].append(mae)
                    all_methods[method]['rows'].append(len(t_train))
                    print(f"   col={col} fold={fold + 1} method={method:6}: MAE={mae:9.2f}, Rows={len(t_train)}")
            else:
                for method in methods:
                    all_methods[method]['maes'].append(np.inf)
                    all_methods[method]['rows'].append(0)

        # Résumé
        summed = {
            method: {
                'mae': np.sum(results['maes']),
                'rows': np.sum(results['rows'])
            } for method, results in all_methods.items()
        }

        best_non_delete = min(['None', 'Mean', 'Mode'], key=lambda m: summed[m]['mae'])
        delete_mae = summed['Delete']['mae']
        delete_rows = summed['Delete']['rows']
        mae_diff = summed[best_non_delete]['mae'] - delete_mae
        row_diff = summed[best_non_delete]['rows'] - delete_rows
        ratio = mae_diff / (row_diff + 1e-9)

        print(f"\n{col} Summary:")
        for method in methods:
            mean_mae = summed[method]['mae'] / 5 if summed[method]['mae'] != np.inf else 'inf'
            print(f"{method:6}: MAE={mean_mae:9}, Rows={summed[method]['rows'] // 5}")

        if ratio > 2 and delete_rows // 5 >= min_lines:
            print(f"✅ Method 'Delete' selected for {col} (ratio={ratio:.2f})\n")
            mapping[col] = 'Delete'
        else:
            print(f"❌ Delete rejected (ratio={ratio:.2f}, rows={delete_rows // 5})")
            print(f"✅ Method '{best_non_delete}' selected for {col}\n")
            mapping[col] = best_non_delete

        return mapping

    def main(train_set, test_set, order):
        target = 'transaction_sale_price'
        features = train_set.columns.drop(target).tolist()

        # MAE avant traitement des NaN
        base_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=2)
        base_model.fit(train_set[features], train_set[target])
        base_preds = base_model.predict(test_set[features])
        base_mae = mean_absolute_error(test_set[target], base_preds)
        mapping_nan = {}

        for col in order:
            if train_set[col].dtype != 'bool':
                if (train_set[col] == -1).sum() > 0:
                    mapping_nan = NaN.nan_management(train_set, col, mapping_nan, min_lines=40_000, n_estimators=80)
        #mapping_nan = {'transaction_certificates_primaryEnergyConsumptionPerSqm':'Mean','property_location_longitude':'Delete','property_roomCount':'Mode','property_location_latitude':'Delete','property_hasSwimmingPool':'Mean','property_location_floor':'Mode','property_building_constructionYear':'Mean','property_parkingCountIndoor':'Mode','property_building_condition':'Mode','property_building_floorCount':'Mean','property_constructionPermit_hasPlotDivisionAuthorization':'Mean','property_hasAirConditioning':'Mean','property_location_hasSeaView':'Mean','property_gardenSurface':'Mode','property_hasDisabledAccess':'Mean','property_hasVisiophone':'Mean','property_hasArmoredDoor':'Mean','transaction_sale_isFurnished':'Mode','property_hasDoorPhone':'Mean','property_constructionPermit_hasObligationToConstruct':'Mean','property_constructionPermit_isObtained':'Mean','terrace_cos':'Mode','property_energy_hasDoubleGlazing':'Mean','property_energy_hasCollectiveWaterHeater':'Mean','property_propertyCertificates_oilTankCertificateStatus':'Mode','property_hasCableTV':'Mean','property_land_iswooded':'Mode','property_specificities_office_surface':'Mode','property_land_hasplottorear':'Mode','property_constructionPermit_hasPossiblePriorityPurchaseRight':'Mean','transaction_certificates_primaryEnergyConsumptionYearly':'Mean','garden_cos':'Mode','property_specificities_hasWorkspace':'Mean','property_propertyCertificates_hasAsbestosCertificate':'Mode','property_energy_hasThermicPanels':'Mode','property_land_isfacingstreet':'Mean','property_attic_isisolated':'Mean','transaction_certificates_carbonEmission':'Mode','flags_isNotarySale':'Mean',}

        print(f"\nMAE initial (sans gestion NaN): {base_mae:.2f}")

        # Appliquer les méthodes choisies
        for col, method in mapping_nan.items():
            train_set = NaN.apply_nan_method(train_set, col, method)
            test_set = NaN.apply_nan_method(test_set, col, method)

        final_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=2)
        final_model.fit(train_set[features], train_set[target])
        final_preds = final_model.predict(test_set[features])
        final_mae = mean_absolute_error(test_set[target], final_preds)

        print(f"✅ MAE après gestion des NaN: {final_mae:.2f}")

        return mapping_nan



print(f"Program started at : {time.strftime('%H:%M:%S')}")
START_TIME = time.time()

df = load_df()
df = df.replace({np.nan:-1})
train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

"""
order = ['transaction_sale_price'] + choose_columns_rf(train_set, 200)
kept_columns, throw_columns = Columns_sorter.main(train_set, order, kf, True)
print(kept_columns, "\n", throw_columns)
kept_columns += ['transaction_sale_price']
train_set, test_set = train_set[kept_columns], test_set[kept_columns]

END_TIME = time.time()
print("="*50,"ORDER COLUMNS :",END_TIME - START_TIME)
"""

order = choose_columns_rf(train_set, 200)
mapping_outliers = Outliers.main(train_set, test_set, order)
print(mapping_outliers)

try:
    for prev_col, prev_method in mapping_outliers.items():
        train_set = Outliers.apply_outlier_method(train_set, prev_col, prev_method)
        test_set = Outliers.apply_outlier_method(test_set, prev_col, prev_method)
except:
    print("= ERROR OUTLIERS = "*400)

END_TIME = time.time()
print("="*50,"OUTLIERS MANAGEMENT :",END_TIME - START_TIME)

order = choose_columns_rf(train_set, 200)
mapping_nan = NaN.main(train_set, test_set, order)
print(mapping_nan)

try:
    for prev_col, prev_method in mapping_nan.items():
        train_set = NaN.apply_nan_method(train_set, prev_col, prev_method)
        test_set = NaN.apply_nan_method(test_set, prev_col, prev_method)
except:
    print("= ERROR NAN = "*400)

END_TIME = time.time()
print("="*50,"NAN MANAGEMENT :",END_TIME - START_TIME)

df = pd.concat([train_set, test_set])
df.to_csv(path.join(path.dirname(__file__),'..', 'data', 'ml_data.csv'))


print(f"Program ended at : {time.strftime('%H:%M:%S')}")


df = load_df()
df = df.replace({np.nan:-1})
model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=4)
target= "transaction_sale_price"
features = df.drop(columns=[target]).columns.tolist()
model.fit(train_set[features], train_set[target])
with open(path.join(path.dirname(__file__), '..', 'data', 'model_reg_rf.pkl'), "wb") as f:
    joblib.dump(model, f)

print(f"Model downloaded")
