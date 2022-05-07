# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your code starts here...

# COMMAND ----------

recommend = spark.sql("""
SELECT ethereumetl.token_prices_usd.name, g07_db.user_recommendations.symbol,links,market_cap_rank,asset_platform_id,image,contract_address
FROM g07_db.user_recommendations
LEFT JOIN ethereumetl.token_prices_usd ON UPPER(g07_db.user_recommendations.symbol) = UPPER(ethereumetl.token_prices_usd.symbol)
ORDER BY market_cap_rank,coingecko_rank ASC
""").toPandas()
recommend = recommend.dropna(subset=['market_cap_rank']).drop_duplicates(subset=['name'])
recommend = recommend[recommend.asset_platform_id=="ethereum"].head(10)
recommend = recommend[['name','symbol','links','image','contract_address']]
recommend.reset_index(drop=True, inplace=True)
recommend

# COMMAND ----------


recommendations = ""
for index, row in recommend.iterrows():
    recommendations += """
    <tr>
      <td style="height:60px"><img src="{2}"></td>          
      <td style="height:60px">{0} ({1})</td>          
      <td style="height:60px"> <a href="https://etherscan.io/address/{3}"> <img src="https://s1.ax1x.com/2022/05/07/OMAEcj.png" alt="OMAEcj.png" width="28" height="28" /> </a> </td>
    </tr>
    """.format(row["name"], row["symbol"], row["image"] , row["contract_address"])
 
displayHTML("""
    <h3>Recommend Tokens for User Address:</h3>
    <span style="color:#92948f">""" + wallet_address + """</span>
    <table style="padding:10px 30px 0 10px">
    {0}
    </table>
""".format(recommendations))

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
