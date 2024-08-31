# Databricks notebook source
df = spark.table("sandbox.med.abt_churnC").toPandas()
df

# COMMAND ----------

df.describe()

# COMMAND ----------

from sklearn import model_selection
target = 'flChurn'
features = df.columns.tolist()[3:]
X = df[features]
y = df[target]

#Separação entre base de treino e teste
X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size = 0.2, random_state = 42)

print("Taxa resposta treino: ", y_train.mean())
print("Taxa resposta teste: ", y_test.mean())

# COMMAND ----------

X_train.isna().sum().sort_values()

df_train = X_train.copy()
df_train[target] = y
df_train.groupby(target).mean().T

# COMMAND ----------

X_train.isna().sum().sort_values()

df_train = X_train.copy()
df_train[target] = y
df_train.groupby(target).mean().T

# COMMAND ----------

from feature_engine import imputation

number = X_train['nrAvgRecorrencia'].max()

imput = imputation.ArbitraryNumberImputer(variables=['nrAvgRecorrencia'],
                                          arbitrary_number=number)

imput.fit(X_train)

X_train_transform = imput.transform(X_train)

X_test_transform = imput.transform(X_test)

# COMMAND ----------

from sklearn import tree
from sklearn import ensemble
from sklearn import metrics
from sklearn import pipeline
from sklearn.impute import SimpleImputer 

modelo = ensemble.AdaBoostClassifier(n_estimators=1000, random_state=42, learning_rate=0.01)

meu_pipeline = pipeline.Pipeline(steps=[
    ('Imputação', imput),
    ('Modelo', modelo)
])

meu_pipeline.fit(X_train, y_train)

y_predito_train = meu_pipeline.predict(X_train)
acc_train = metrics.accuracy_score(y_train, y_predito_train)
print("Acurácia Treino:", acc_train)

y_predito_test = meu_pipeline.predict(X_test)
acc_test = metrics.accuracy_score(y_test, y_predito_test)
print("Acurácia Teste:", acc_test)

# COMMAND ----------

import pandas as pd
meu_modelo = pd.Series({"modelo": meu_pipeline})
meu_modelo.to_pickle("meu_modelo.pkl")

# COMMAND ----------

import matplotlib.pyplot as plt

y_proba_train = meu_pipeline.predict_proba(X_train)
y_proba_train_churn = y_proba_train[:,1]

auc_train = metrics.roc_auc_score(y_train, y_proba_train_churn)
print("AUC Treino:", auc_train)

fpr_train, tpr_train, _ = metrics.roc_curve(y_train, y_proba_train_churn)

y_proba_test = meu_pipeline.predict_proba(X_test)
y_proba_test_churn = y_proba_test[:,1]

auc_test = metrics.roc_auc_score(y_test, y_proba_test_churn)
print("AUC Test:", auc_test)

fpr_test, tpr_test, _ = metrics.roc_curve(y_test, y_proba_test_churn)

plt.plot(fpr_train, tpr_train, color='green')
plt.plot(fpr_test, tpr_test, color='blue')
plt.plot([0,1], [0,1], '--', color='black')
plt.grid()
plt.legend([f"Base Treino: {auc_train:.4f}", f"Base Teste: {auc_test:.4f}"])
plt.show()
