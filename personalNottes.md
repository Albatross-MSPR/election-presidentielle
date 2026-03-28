
check appache air flow for pipline
checl datset ffrom mohammad on discourd

take a look at dtale might be helpful for showing tbales








The 17 Communes that Disappeared (2017 → 2022)
These are Beaujolais wine region communes that merged together:
Merged into new communes:

Belleville (4,714 inscrits) → became Belleville-en-Beaujolais (8,451 inscrits) ✅
Pontcharra-sur-Turdine (2,025) → merged with others
Saint-Jean-d'Ardières (2,739) → likely merged
Saint-Andéol-le-Château (1,276) → merged
Plus 13 smaller communes (630-1,276 inscrits each)

Total disappeared: 16,199 inscrits
The 4 New Communes in 2022
These are the results of those mergers:

Belleville-en-Beaujolais (8,451 inscrits) ← Merged from old "Belleville" + others
Beauvallon (3,086) ← New merged entity
Deux-Grosnes (1,343) ← New merged entity
Vindry-sur-Turdine (3,931) ← New merged entity


====================================================================================================
🔍 DETAILED COMMUNE ANALYSIS: 2017 T2 vs 2022 T2

📍 COMMUNES ONLY IN 2017 T2 (17 communes)
These communes existed in 2017 but NOT in 2022
Possible reasons: Merged with other communes, boundary changes, or data collection differences

 1. Avenas                                        | Inscrits:     115 | Votants:      95
 2. Belleville                                    | Inscrits:   4,714 | Votants:   3,412
 3. Chassagny                                     | Inscrits:   1,058 | Votants:     881
 4. Dareizé                                       | Inscrits:     370 | Votants:     299
 5. Jarnioux                                      | Inscrits:     495 | Votants:     404
 6. Les Olmes                                     | Inscrits:     641 | Votants:     519
 7. Monsols                                       | Inscrits:     591 | Votants:     431
 8. Ouroux                                        | Inscrits:     242 | Votants:     194
 9. Pontcharra-sur-Turdine                        | Inscrits:   2,025 | Votants:   1,505
10. Saint-Andéol-le-Château                       | Inscrits:   1,276 | Votants:   1,051
11. Saint-Christophe-la-Montagne                  | Inscrits:     198 | Votants:     147
12. Saint-Jacques-des-Arrêts                      | Inscrits:      90 | Votants:      69
13. Saint-Jean-d'Ardières                         | Inscrits:   2,739 | Votants:   2,079
14. Saint-Jean-de-Touslas                         | Inscrits:     630 | Votants:     552
15. Saint-Loup                                    | Inscrits:     857 | Votants:     699
16. Saint-Mamert                                  | Inscrits:      59 | Votants:      52
17. Trades                                        | Inscrits:      99 | Votants:      87


📍 COMMUNES ONLY IN 2022 T2 (4 communes)
These communes existed in 2022 but NOT in 2017
Possible reasons: New communes from mergers, administrative changes, or better data coverage

 1. Beauvallon                                    | Inscrits:   3,086 | Votants:   2,478
 2. Belleville-en-Beaujolais                      | Inscrits:   8,451 | Votants:   5,945
 3. Deux-Grosnes                                  | Inscrits:   1,343 | Votants:   1,044
 4. Vindry-sur-Turdine                            | Inscrits:   3,931 | Votants:   3,108


DONT FORGET TO FIX THE MISSING COMMUNES AFTER TRAINING THE MODEL 
code to keep only comon commune are in perso




Historiquement, le décompte séparé des bulletins blancs et des bulletins nuls n'a été appliqué en France qu'à partir de la loi de 2014. C'est pourquoi le fichier de 2012 contient la colonne agrégée "Blancs et nuls", alors que ceux de 2017 et 2022 les séparent.

Il est préférable d'utiliser des valeurs nulles (NULL / NaN) lorsque la granularité de l'information n'existe pas, et d'ajouter une colonne pour englober la notion historique.

Ajoutez une colonne blancs_et_nuls à votre schéma Silver global.
Pour 2012 : blancs_et_nuls reçoit la valeur source. Les colonnes blancs et nuls reçoivent la valeur NULL (et non 0).
Pour 2017 et 2022 : blancs et nuls reçoivent leurs valeurs sources respectives. Vous pouvez aussi calculer blancs_et_nuls = blancs + nuls pour faciliter les comparaisons temporelles dans vos futurs tableaux de bord.


