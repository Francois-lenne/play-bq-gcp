gcloud functions deploy data_playstation \
--runtime python310 \
--trigger-http \
--allow-unauthenticated \
--entry-point main \
--set-secrets DATASET_NAME='projects/629847393464/secrets/DATASET_NAME/versions/latest',GOOGLE_APPLICATION_CREDENTIALS='projects/629847393464/secrets/GOOGLE_APPLICATION_CREDENTIALS/versions/latest',TABLE_NAME_GAME='projects/629847393464/secrets/TABLE_NAME_GAME/versions/latest',TABLE_NAME_TIME_PLAY='projects/629847393464/secrets/TABLE_NAME_TIME_PLAY/versions/latest',TABLE_NAME_TROPHEE='projects/629847393464/secrets/TABLE_NAME_TROPHEE/versions/latest',project_id='projects/629847393464/secrets/project_id/versions/latest',psn='projects/629847393464/secrets/psn/versions/latest'

gcloud scheduler jobs create http morning_job_playsation \
--schedule "0 8 * * *" \
--http-method GET \
--uri "URL_cloud_function" \
--oidc-service-account-email "service-account-email" \



