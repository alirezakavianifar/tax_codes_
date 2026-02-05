import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_score, GridSearchCV
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import BaggingClassifier
import pickle


def func1(x):
    return x[:5]


def preprocess_data(paths, how_imbalance='oversampling'):
    lst = []
    for key, path in paths.items():
        if key == 'eghtesadi':
            df_eghtesadi = pd.read_excel(path, sheet_name='Sheet2')
            df_gasht = pd.read_excel(path, sheet_name='Sheet1')
            df_gasht = df_gasht.astype('str')
            df_eghtesadi.dropna(subset=['کد پستی'], inplace=True)

            df_eghtesadi['posti'] = df_eghtesadi['کد پستی'].apply(
                lambda x: func1(str(x)))
            df_eghtesadi['آدرس'] = df_eghtesadi['شهر'].astype(
                'str') + ' ' + df_eghtesadi['آدرس'].astype('str')

            df_merged = df_eghtesadi.merge(
                df_gasht, how='inner', left_on='posti', right_on='گشت پستی')

            df_merged['کد اداره امور مالیاتی'] = df_merged['کد اداره امور مالیاتی'].astype(
                'int')
            df_merged.rename(
                columns={'کد اداره امور مالیاتی': 'اداره'}, inplace=True)

            df_merged = df_merged[[
                'اداره', 'آدرس']]

            df_merged['اداره'] = df_merged['اداره'].astype('int')

            lst.append(df_merged)
            continue

        df = pd.read_excel(path)

        df['اداره'] = df['اداره'].astype('int')
        df.dropna(subset=['آدرس'], inplace=True)

        lst.append(df)

    df = pd.concat(lst)

    if how_imbalance == 'oversampling':
        dfs = []
        num_major = df['اداره'].value_counts().max()
        for item in df['اداره'].value_counts().index.tolist():
            df_new = df[df['اداره'] == item].sample(
                num_major, replace=True)
            dfs.append(df_new)

        df = pd.concat(dfs)

    X_train, X_test, y_train, y_test = train_test_split(
        df['آدرس'],
        df['اداره'],
        test_size=0.2,
        random_state=2022,
        stratify=df['اداره']
    )

    return X_train, X_test, y_train, y_test


if __name__ == '__main__':

    path = r'E:\automating_reports_V2\saved_dir\codeghtesadi\44.xlsx'
    path2 = r'E:\automating_reports_V2\automation\text_classification\رسیدگی شده مستغلات 1400.xlsx'
    paths = {'eghtesadi': path, 'mostaghelat': path2}

    df = preprocess_data(paths)

    X_train, X_test, y_train, y_test = preprocess_data(paths)

    clf = Pipeline(
        [
            ('vectorizer_bow', CountVectorizer(ngram_range=(1, 4))),
            ('Multi_NB', MultinomialNB())
        ]
    )

    model = clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    print(classification_report(y_test, y_pred))

    with open('saved_model_ngram1_4_retrain_', 'wb') as f:
        pickle.dump(model, f)

    # clf = Pipeline(
    #     [
    #         ('vectorizer_bow', CountVectorizer(ngram_range=(1, 4))),
    #         ('Multi_NB', BaggingClassifier(base_estimator=MultinomialNB(),
    #                                        n_estimators=5,
    #                                        max_samples=0.7,
    #                                        oob_score=True,
    #                                        random_state=0))
    #     ]
    # )

    # model = clf.fit(X_train, y_train)

    # y_pred = clf.predict(X_test)

    # print(classification_report(y_test, y_pred))

    with open('saved_model_ngram1_4_retrain_baging', 'wb') as f:
        pickle.dump(model, f)
