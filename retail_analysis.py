import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class RetailAnalysisPipeline:
    def __init__(self):
        """Initialize Spark with optimized configuration"""
        # Create temp directories
        os.makedirs("E:/Retail Analysis/spark-temp", exist_ok=True)
        os.makedirs("E:/Retail Analysis/spark-warehouse", exist_ok=True)

        self.spark = SparkSession.builder \
            .appName("Retail Sales Analysis") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.storage.level", "MEMORY_ONLY") \
            .config("spark.local.dir", "E:/Retail Analysis/spark-temp") \
            .config("spark.sql.warehouse.dir", "E:/Retail Analysis/spark-warehouse") \
            .master("local[*]") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Set plot style
        plt.style.use('default')
        sns.set_style("whitegrid")

    def load_and_clean_data(self, file_path):
        """Load and clean the retail sales data"""
        print(f"Loading data from: {file_path}")
        
        # Define schema with spaces in column names
        schema = StructType([
            StructField("Region", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Item Type", StringType(), True),
            StructField("Sales Channel", StringType(), True),
            StructField("Order Priority", StringType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Order ID", StringType(), True),
            StructField("Ship Date", StringType(), True),
            StructField("Units Sold", IntegerType(), True),
            StructField("Unit Price", DoubleType(), True),
            StructField("Unit Cost", DoubleType(), True),
            StructField("Total Revenue", DoubleType(), True),
            StructField("Total Cost", DoubleType(), True),
            StructField("Total Profit", DoubleType(), True)
        ])

        try:
            # Load data with correct column names
            self.df = self.spark.read.csv(
                file_path,
                header=True,
                schema=schema
            )

            # Clean and transform data
            self.df = self.df \
                .dropDuplicates(['Order ID']) \
                .dropna() \
                .withColumn('Order_Date', to_date('Order Date', 'M/d/yyyy')) \
                .withColumn('Ship_Date', to_date('Ship Date', 'M/d/yyyy')) \
                .withColumn('Year', year('Order_Date')) \
                .withColumn('Month', month('Order_Date')) \
                .withColumn('Profit_Margin', col('Total Profit') / col('Total Revenue') * 100) \
                .cache()
            
            count = self.df.count()
            print(f"Successfully loaded and cleaned {count:,} records")
            return self

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise

    def create_visualizations(self, output_path):
        """Create and save visualizations"""
        print("Creating visualizations...")
        os.makedirs(output_path, exist_ok=True)

        try:
            # 1. Regional Performance Heatmap
            print("1. Creating Regional Performance Heatmap...")
            regional_data = self.df.groupBy('Region') \
                .agg(
                    sum('Total Revenue').alias('Total_Revenue'),
                    sum('Total Profit').alias('Total_Profit'),
                    count('Order ID').alias('Order_Count')
                ).toPandas()
            
            plt.figure(figsize=(12, 6))
            sns.heatmap(
                regional_data.set_index('Region')[['Total_Revenue', 'Total_Profit', 'Order_Count']],
                annot=True, fmt='.0f', cmap='YlOrRd'
            )
            plt.title('Regional Performance Overview')
            plt.tight_layout()
            plt.savefig(os.path.join(output_path, 'regional_heatmap.png'))
            plt.close()

            # 2. Monthly Sales Trend
            print("2. Creating Monthly Sales Trend...")
            monthly_trends = self.df.groupBy('Year', 'Month') \
                .agg(sum('Total Revenue').alias('Monthly_Revenue')) \
                .orderBy('Year', 'Month') \
                .toPandas()
            
            monthly_trends['Date'] = pd.to_datetime(
                monthly_trends['Year'].astype(str) + '-' + 
                monthly_trends['Month'].astype(str) + '-01'
            )
            
            plt.figure(figsize=(15, 6))
            plt.plot(monthly_trends['Date'], monthly_trends['Monthly_Revenue'])
            plt.title('Monthly Sales Trend')
            plt.xlabel('Date')
            plt.ylabel('Revenue')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_path, 'monthly_trend.png'))
            plt.close()

            # 3. Product Category Performance
            print("3. Creating Product Category Performance Chart...")
            product_performance = self.df.groupBy('Item Type') \
                .agg(
                    sum('Total Profit').alias('Total_Profit'),
                    avg('Profit_Margin').alias('Avg_Margin')
                ).toPandas()
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(name='Total Profit', x=product_performance['Item Type'], 
                      y=product_performance['Total_Profit']),
                secondary_y=False
            )
            fig.add_trace(
                go.Scatter(name='Avg Margin', x=product_performance['Item Type'], 
                          y=product_performance['Avg_Margin'], mode='lines+markers'),
                secondary_y=True
            )
            fig.update_layout(title='Product Category Performance', barmode='group')
            fig.write_html(os.path.join(output_path, 'product_performance.html'))

            # 4. Sales Channel Distribution
            print("4. Creating Sales Channel Distribution...")
            channel_dist = self.df.groupBy('Sales Channel') \
                .agg(sum('Total Revenue').alias('Revenue')) \
                .toPandas()
            
            plt.figure(figsize=(10, 8))
            plt.pie(channel_dist['Revenue'], labels=channel_dist['Sales Channel'], 
                    autopct='%1.1f%%')
            plt.title('Revenue Distribution by Sales Channel')
            plt.savefig(os.path.join(output_path, 'channel_distribution.png'))
            plt.close()

            # 5. Order Priority Distribution
            print("5. Creating Order Priority Distribution...")
            priority_analysis = self.df.groupBy('Region', 'Order Priority') \
                .agg(count('Order ID').alias('Order_Count')) \
                .toPandas()
            
            priority_pivot = priority_analysis.pivot(
                index='Region', columns='Order Priority', values='Order_Count'
            ).fillna(0)
            
            ax = priority_pivot.plot(kind='bar', stacked=True, figsize=(12, 6))
            plt.title('Order Priority Distribution by Region')
            plt.xlabel('Region')
            plt.ylabel('Number of Orders')
            plt.legend(title='Order Priority', bbox_to_anchor=(1.05, 1))
            plt.tight_layout()
            plt.savefig(os.path.join(output_path, 'priority_distribution.png'))
            plt.close()

            # 6. Shipping Efficiency
            print("6. Creating Shipping Efficiency Analysis...")
            shipping_data = self.df.select('Region', 
                datediff('Ship_Date', 'Order_Date').alias('Shipping_Days')
            ).toPandas()
            
            plt.figure(figsize=(12, 6))
            sns.boxplot(x='Region', y='Shipping_Days', data=shipping_data)
            plt.title('Shipping Days Distribution by Region')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_path, 'shipping_efficiency.png'))
            plt.close()

            print("All visualizations created successfully!")

        except Exception as e:
            print(f"Error creating visualizations: {str(e)}")
            raise

def cleanup_temp_dirs():
    """Clean up temporary directories"""
    temp_dirs = [
        "E:/Retail Analysis/spark-temp",
        "E:/Retail Analysis/spark-warehouse"
    ]
    for dir_path in temp_dirs:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
            except Exception as e:
                print(f"Warning: Could not remove temp directory {dir_path}: {str(e)}")

def main():
    try:
        # Set up paths
        base_path = "E:\\Retail Analysis"
        input_file = "expanded_5m_sales_records.csv"
        input_path = os.path.join(base_path, input_file)
        output_path = os.path.join(base_path, "retail_analysis_results")
        
        # Clean up any existing temp directories
        cleanup_temp_dirs()
        
        print(f"Starting Retail Sales Analysis...")
        print(f"Input path: {input_path}")
        print(f"Output path: {output_path}")
        
        # Initialize and run pipeline
        pipeline = RetailAnalysisPipeline()
        pipeline.load_and_clean_data(input_path)
        pipeline.create_visualizations(output_path)
        
        # Final cleanup
        cleanup_temp_dirs()
        
        print("\nAnalysis completed successfully!")
        print(f"Results saved in: {output_path}")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        
        print("\nTroubleshooting steps:")
        print("1. Ensure enough disk space (at least 10GB free)")
        print("2. Verify all required packages are installed:")
        print("   pip install pandas matplotlib seaborn plotly")
        print(f"3. Check file exists: {input_path}")
        print("4. Check write permissions in output directory")
        
        # Cleanup on error
        cleanup_temp_dirs()
        sys.exit(1)

if __name__ == "__main__":
    main()