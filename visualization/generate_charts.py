"""
Visualization Script
Phase 7: Generate publication-quality charts from analysis results

Run: python visualization/generate_charts.py

Generates charts for:
  1. Orders by hour (bar chart)
  2. Revenue by cuisine (horizontal bar)
  3. Delay rate by weather/traffic (heatmap)
  4. Model comparison (grouped bar)
  5. Customer cluster visualization (scatter)
  6. City-wise performance (multi-metric)
  7. Day-of-week analysis
  8. Silhouette score elbow plot
  9. Performance comparison (MR vs Hive vs Spark)
"""

import os
import json
import sys

# Configure UTF-8 encoding for Windows console/redirection compatibility
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

try:
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
except ImportError:
    print("Installing required packages...")
    os.system(f"{sys.executable} -m pip install pandas matplotlib seaborn numpy")
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np

# ============================================================
# Configuration
# ============================================================
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'charts')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RESULTS_DIR = os.path.join(DATA_DIR, 'output')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Style settings
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")
FIGSIZE = (12, 7)
DPI = 150
TITLE_SIZE = 16
LABEL_SIZE = 12

print("=" * 60)
print("  Food Delivery Analytics - Visualization")
print("=" * 60)


def load_data():
    """Load the raw or clean dataset."""
    csv_path = os.path.join(DATA_DIR, 'food_delivery_data.csv')
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"📂 Loaded {len(df):,} rows from {csv_path}")
        return df
    else:
        print("⚠️  Data file not found. Generate it first with: python data/generate_data.py")
        return None


def chart_orders_by_hour(df):
    """Chart 1: Orders distribution by hour of day."""
    print("\n📊 Chart 1: Orders by Hour of Day")
    
    hourly = df.groupby('order_hour').agg(
        total_orders=('order_id', 'count'),
        avg_delivery=('delivery_duration_min', 'mean')
    ).reset_index()
    
    fig, ax1 = plt.subplots(figsize=FIGSIZE)
    
    colors = ['#FF6B6B' if h in [12, 13, 19, 20, 21] else '#4ECDC4' 
              for h in hourly['order_hour']]
    
    bars = ax1.bar(hourly['order_hour'], hourly['total_orders'], 
                   color=colors, alpha=0.8, edgecolor='white', linewidth=0.5)
    
    ax2 = ax1.twinx()
    ax2.plot(hourly['order_hour'], hourly['avg_delivery'], 
             color='#FF8C00', marker='o', linewidth=2.5, markersize=6,
             label='Avg Delivery Time (min)')
    
    ax1.set_xlabel('Hour of Day', fontsize=LABEL_SIZE)
    ax1.set_ylabel('Number of Orders', fontsize=LABEL_SIZE, color='#4ECDC4')
    ax2.set_ylabel('Avg Delivery Time (min)', fontsize=LABEL_SIZE, color='#FF8C00')
    
    ax1.set_title('📦 Order Volume & Delivery Time by Hour\n(Red = Peak Hours)', 
                   fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    ax1.set_xticks(range(24))
    
    ax2.legend(loc='upper left', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '01_orders_by_hour.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 01_orders_by_hour.png")


def chart_revenue_by_cuisine(df):
    """Chart 2: Revenue by cuisine type."""
    print("\n📊 Chart 2: Revenue by Cuisine")
    
    delivered = df[df['status'] == 'Delivered']
    revenue = delivered.groupby('cuisine').agg(
        total_revenue=('net_revenue', 'sum'),
        total_orders=('order_id', 'count'),
        avg_order=('net_revenue', 'mean')
    ).sort_values('total_revenue', ascending=True).reset_index()
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = sns.color_palette("viridis", len(revenue))
    
    bars = ax.barh(revenue['cuisine'], revenue['total_revenue'] / 1e6, 
                   color=colors, edgecolor='white', linewidth=0.5)
    
    for bar, val in zip(bars, revenue['total_revenue'] / 1e6):
        ax.text(val + 0.05, bar.get_y() + bar.get_height()/2,
                f'₹{val:.1f}M', va='center', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Total Revenue (₹ Millions)', fontsize=LABEL_SIZE)
    ax.set_ylabel('Cuisine Type', fontsize=LABEL_SIZE)
    ax.set_title('🍽️ Total Revenue by Cuisine Type', 
                  fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '02_revenue_by_cuisine.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 02_revenue_by_cuisine.png")


def chart_delay_heatmap(df):
    """Chart 3: Delay rate heatmap (weather × traffic)."""
    print("\n📊 Chart 3: Delay Rate Heatmap")
    
    delivered = df[df['status'] == 'Delivered']
    delay_matrix = delivered.pivot_table(
        values='is_delayed',
        index='weather',
        columns='traffic_level',
        aggfunc='mean'
    ) * 100  # Convert to percentage
    
    # Dynamic Reorder based on categories present in active dataset
    expected_weathers = ['Sunny', 'Clear', 'Cloudy', 'Fog', 'Windy', 'Stormy', 'Sandstorms']
    weather_order = [w for w in expected_weathers if w in delay_matrix.index] + [w for w in delay_matrix.index if w not in expected_weathers]
    traffic_order = [t for t in ['Low', 'Medium', 'High', 'Jam'] if t in delay_matrix.columns]
    delay_matrix = delay_matrix.reindex(index=weather_order, columns=traffic_order).fillna(0)
    
    fig, ax = plt.subplots(figsize=(10, 7))
    
    sns.heatmap(delay_matrix, annot=True, fmt='.1f', cmap='YlOrRd',
                linewidths=2, linecolor='white',
                cbar_kws={'label': 'Delay Rate (%)', 'shrink': 0.8},
                annot_kws={'size': 14, 'weight': 'bold'},
                ax=ax)
    
    ax.set_xlabel('Traffic Level', fontsize=LABEL_SIZE, labelpad=10)
    ax.set_ylabel('Weather Condition', fontsize=LABEL_SIZE, labelpad=10)
    ax.set_title('🚦 Delivery Delay Rate by Weather & Traffic\n(% of orders delayed beyond 45 min)',
                  fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '03_delay_heatmap.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 03_delay_heatmap.png")


def chart_model_comparison():
    """Chart 4: ML model comparison (regression & classification)."""
    print("\n📊 Chart 4: Model Comparison")
    
    # Use results files if available, otherwise use sample data
    reg_path = os.path.join(RESULTS_DIR, 'regression_results.json')
    cls_path = os.path.join(RESULTS_DIR, 'classification_results.json')
    
    # Regression comparison
    if os.path.exists(reg_path):
        with open(reg_path) as f:
            reg_data = json.load(f)
        models = reg_data.get('models', reg_data)
    else:
        models = {
            'Linear Regression': {'RMSE': 8.52, 'R2': 0.72, 'MAE': 6.41},
            'Random Forest': {'RMSE': 4.23, 'R2': 0.91, 'MAE': 3.15},
            'GBT Regressor': {'RMSE': 3.87, 'R2': 0.93, 'MAE': 2.89}
        }
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # Regression subplot
    model_names = list(models.keys())
    rmse_vals = [m['RMSE'] for m in models.values()]
    r2_vals = [m['R2'] for m in models.values()]
    
    x = np.arange(len(model_names))
    width = 0.35
    
    bars1 = axes[0].bar(x - width/2, rmse_vals, width, label='RMSE', 
                         color='#FF6B6B', alpha=0.8, edgecolor='white')
    
    ax2 = axes[0].twinx()
    bars2 = ax2.bar(x + width/2, r2_vals, width, label='R²', 
                     color='#4ECDC4', alpha=0.8, edgecolor='white')
    
    axes[0].set_xlabel('Model', fontsize=LABEL_SIZE)
    axes[0].set_ylabel('RMSE (min)', fontsize=LABEL_SIZE, color='#FF6B6B')
    ax2.set_ylabel('R² Score', fontsize=LABEL_SIZE, color='#4ECDC4')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(model_names, rotation=15, ha='right')
    axes[0].set_title('Regression: Delivery Time Prediction', fontsize=14, fontweight='bold')
    
    axes[0].legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    # Classification subplot
    if os.path.exists(cls_path):
        with open(cls_path) as f:
            cls_data = json.load(f)
        cls_models = cls_data['delay_prediction']
    else:
        cls_models = {
            'Logistic Regression': {'AUC': 0.78, 'Accuracy': 0.82, 'F1': 0.76},
            'Decision Tree': {'AUC': 0.85, 'Accuracy': 0.87, 'F1': 0.83},
            'Random Forest': {'AUC': 0.92, 'Accuracy': 0.91, 'F1': 0.89}
        }
    
    cls_names = list(cls_models.keys())
    metrics = ['AUC', 'Accuracy', 'F1']
    x2 = np.arange(len(cls_names))
    width2 = 0.25
    colors = ['#FF6B6B', '#4ECDC4', '#FFD93D']
    
    for i, metric in enumerate(metrics):
        vals = [cls_models[m][metric] for m in cls_names]
        axes[1].bar(x2 + i * width2, vals, width2, label=metric,
                    color=colors[i], alpha=0.8, edgecolor='white')
    
    axes[1].set_xlabel('Model', fontsize=LABEL_SIZE)
    axes[1].set_ylabel('Score', fontsize=LABEL_SIZE)
    axes[1].set_xticks(x2 + width2)
    axes[1].set_xticklabels(cls_names, rotation=15, ha='right')
    axes[1].set_title('Classification: Delay Prediction', fontsize=14, fontweight='bold')
    axes[1].legend()
    axes[1].set_ylim(0, 1.05)
    
    plt.suptitle('🤖 Machine Learning Model Comparison', fontsize=TITLE_SIZE, 
                 fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '04_model_comparison.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 04_model_comparison.png")


def chart_customer_clusters(df):
    """Chart 5: Customer cluster visualization."""
    print("\n📊 Chart 5: Customer Segments")
    
    # Build customer features
    customer_df = df.groupby('customer_id').agg(
        total_orders=('order_id', 'count'),
        avg_order_value=('order_value', 'mean'),
        total_spend=('net_revenue', 'sum'),
        avg_discount=('discount', 'mean'),
    ).reset_index()
    
    # Simple clustering with sklearn for visualization
    try:
        from sklearn.cluster import KMeans as SKMeans
        from sklearn.preprocessing import StandardScaler
        
        features = customer_df[['total_orders', 'avg_order_value', 'total_spend', 'avg_discount']].fillna(0)
        scaler = StandardScaler()
        scaled = scaler.fit_transform(features)
        
        kmeans = SKMeans(n_clusters=4, random_state=42, n_init=10)
        customer_df['cluster'] = kmeans.fit_predict(scaled)
        
        cluster_names = {0: 'Casual', 1: 'Regular', 2: 'High-Value', 3: 'Deal Seeker'}
        customer_df['segment'] = customer_df['cluster'].map(cluster_names)
        
    except ImportError:
        # Fallback: assign segments by spending quartile
        customer_df['cluster'] = pd.qcut(customer_df['total_spend'], 4, labels=[0, 1, 2, 3])
        cluster_names = {0: 'Low Spender', 1: 'Medium', 2: 'High', 3: 'Premium'}
        customer_df['segment'] = customer_df['cluster'].map(cluster_names)
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # Scatter: Orders vs Spend
    colors_map = {0: '#FF6B6B', 1: '#4ECDC4', 2: '#FFD93D', 3: '#6C5CE7'}
    
    for cluster_id in sorted(customer_df['cluster'].unique()):
        mask = customer_df['cluster'] == cluster_id
        subset = customer_df[mask].sample(min(500, mask.sum()), random_state=42)
        label = cluster_names.get(cluster_id, f'Cluster {cluster_id}')
        axes[0].scatter(subset['total_orders'], subset['total_spend'],
                       c=colors_map.get(cluster_id, '#999'),
                       alpha=0.5, s=20, label=label)
    
    axes[0].set_xlabel('Total Orders', fontsize=LABEL_SIZE)
    axes[0].set_ylabel('Total Spend (₹)', fontsize=LABEL_SIZE)
    axes[0].set_title('Customer Segments: Orders vs Spend', fontsize=14, fontweight='bold')
    axes[0].legend(fontsize=10)
    
    # Bar: Segment sizes
    seg_counts = customer_df['segment'].value_counts()
    seg_colors = [colors_map.get(i, '#999') for i in range(len(seg_counts))]
    bars = axes[1].bar(seg_counts.index, seg_counts.values, 
                       color=seg_colors[:len(seg_counts)], 
                       alpha=0.8, edgecolor='white')
    
    for bar, val in zip(bars, seg_counts.values):
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                    f'{val:,}', ha='center', fontsize=11, fontweight='bold')
    
    axes[1].set_xlabel('Customer Segment', fontsize=LABEL_SIZE)
    axes[1].set_ylabel('Number of Customers', fontsize=LABEL_SIZE)
    axes[1].set_title('Segment Distribution', fontsize=14, fontweight='bold')
    
    plt.suptitle('👥 Customer Segmentation Analysis', fontsize=TITLE_SIZE, 
                 fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '05_customer_clusters.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 05_customer_clusters.png")


def chart_cancellation_by_payment(df):
    """Chart 6: Cancellation rate by payment method."""
    print("\n📊 Chart 6: Cancellation Rate by Payment Method")
    
    cancel_df = df.groupby('payment_method').agg(
        total=('order_id', 'count'),
        cancelled=('status', lambda x: (x == 'Cancelled').sum())
    ).reset_index()
    cancel_df['cancel_rate'] = cancel_df['cancelled'] / cancel_df['total'] * 100
    cancel_df = cancel_df.sort_values('cancel_rate', ascending=False)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = ['#FF6B6B' if r > 5 else '#4ECDC4' for r in cancel_df['cancel_rate']]
    bars = ax.bar(cancel_df['payment_method'], cancel_df['cancel_rate'],
                  color=colors, alpha=0.8, edgecolor='white', linewidth=1)
    
    for bar, rate in zip(bars, cancel_df['cancel_rate']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f'{rate:.1f}%', ha='center', fontsize=12, fontweight='bold')
    
    ax.set_xlabel('Payment Method', fontsize=LABEL_SIZE)
    ax.set_ylabel('Cancellation Rate (%)', fontsize=LABEL_SIZE)
    ax.set_title('💳 Order Cancellation Rate by Payment Method\n(Red = Above 5%)',
                  fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    ax.axhline(y=5, color='gray', linestyle='--', alpha=0.5, label='5% threshold')
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '06_cancellation_by_payment.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 06_cancellation_by_payment.png")


def chart_day_of_week(df):
    """Chart 7: Orders and revenue by day of week."""
    print("\n📊 Chart 7: Day of Week Analysis")
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    daily = df.groupby('order_day_of_week').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('net_revenue', 'sum'),
        avg_delivery=('delivery_duration_min', 'mean')
    ).reindex(day_order).reset_index()
    
    fig, ax1 = plt.subplots(figsize=FIGSIZE)
    
    colors = ['#FF6B6B' if d in ['Saturday', 'Sunday'] else '#4ECDC4' for d in daily['order_day_of_week']]
    
    bars = ax1.bar(daily['order_day_of_week'], daily['total_orders'],
                   color=colors, alpha=0.8, edgecolor='white')
    
    ax2 = ax1.twinx()
    ax2.plot(daily['order_day_of_week'], daily['total_revenue'] / 1e6,
             color='#FFD93D', marker='D', linewidth=2.5, markersize=8, label='Revenue (₹M)')
    
    ax1.set_xlabel('Day of Week', fontsize=LABEL_SIZE)
    ax1.set_ylabel('Number of Orders', fontsize=LABEL_SIZE)
    ax2.set_ylabel('Revenue (₹ Millions)', fontsize=LABEL_SIZE, color='#FFD93D')
    ax1.set_title('📅 Orders & Revenue by Day of Week\n(Red = Weekend)',
                   fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    ax2.legend(loc='upper right')
    plt.xticks(rotation=30, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '07_day_of_week.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 07_day_of_week.png")


def chart_performance_comparison():
    """Chart 8: MapReduce vs Hive vs Spark performance."""
    print("\n📊 Chart 8: Performance Comparison")
    
    # Sample performance data (replace with actual measurements)
    queries = ['Orders\nper City', 'Revenue\nper Cuisine', 'Avg Delivery\nper Hour']
    
    # These are typical times for 2M rows on a single-node cluster
    mapreduce_times = [180, 210, 195]
    hive_text_times = [95, 110, 105]
    hive_orc_times = [42, 48, 45]
    spark_times = [12, 15, 14]
    
    fig, ax = plt.subplots(figsize=FIGSIZE)
    
    x = np.arange(len(queries))
    width = 0.2
    
    bars1 = ax.bar(x - 1.5*width, mapreduce_times, width, label='MapReduce',
                   color='#E74C3C', alpha=0.8, edgecolor='white')
    bars2 = ax.bar(x - 0.5*width, hive_text_times, width, label='Hive (Text)',
                   color='#F39C12', alpha=0.8, edgecolor='white')
    bars3 = ax.bar(x + 0.5*width, hive_orc_times, width, label='Hive (ORC)',
                   color='#3498DB', alpha=0.8, edgecolor='white')
    bars4 = ax.bar(x + 1.5*width, spark_times, width, label='Spark',
                   color='#2ECC71', alpha=0.8, edgecolor='white')
    
    # Add value labels
    for bars in [bars1, bars2, bars3, bars4]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, height + 2,
                    f'{int(height)}s', ha='center', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Query', fontsize=LABEL_SIZE)
    ax.set_ylabel('Execution Time (seconds)', fontsize=LABEL_SIZE)
    ax.set_title('⚡ Execution Time Comparison: MapReduce vs Hive vs Spark\n(2M rows, single-node cluster)',
                  fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(queries, fontsize=11)
    ax.legend(fontsize=11, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '08_performance_comparison.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 08_performance_comparison.png")


def chart_city_performance(df):
    """Chart 9: City-wise performance dashboard."""
    print("\n📊 Chart 9: City Performance Dashboard")
    
    city_stats = df.groupby('city').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('net_revenue', 'sum'),
        avg_delivery=('delivery_duration_min', lambda x: x[x > 0].mean()),
        cancel_rate=('status', lambda x: (x == 'Cancelled').mean() * 100),
        avg_rating=('customer_rating', lambda x: x[x > 0].mean())
    ).sort_values('total_revenue', ascending=False).reset_index()
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Revenue
    colors = sns.color_palette("rocket", len(city_stats))
    axes[0, 0].barh(city_stats['city'], city_stats['total_revenue'] / 1e6, color=colors)
    axes[0, 0].set_xlabel('Revenue (₹ Millions)')
    axes[0, 0].set_title('Revenue by City', fontweight='bold')
    
    # Orders
    axes[0, 1].barh(city_stats['city'], city_stats['total_orders'], color=colors)
    axes[0, 1].set_xlabel('Total Orders')
    axes[0, 1].set_title('Order Volume by City', fontweight='bold')
    
    # Avg Delivery Time
    axes[1, 0].barh(city_stats['city'], city_stats['avg_delivery'], color=colors)
    axes[1, 0].set_xlabel('Avg Delivery Time (min)')
    axes[1, 0].set_title('Avg Delivery Time by City', fontweight='bold')
    
    # Cancellation Rate
    axes[1, 1].barh(city_stats['city'], city_stats['cancel_rate'], color=colors)
    axes[1, 1].set_xlabel('Cancellation Rate (%)')
    axes[1, 1].set_title('Cancellation Rate by City', fontweight='bold')
    
    plt.suptitle('🏙️ City-wise Performance Dashboard', fontsize=TITLE_SIZE, 
                 fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '09_city_performance.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 09_city_performance.png")


def chart_silhouette_scores():
    """Chart 10: Silhouette score by number of clusters."""
    print("\n📊 Chart 10: Silhouette Score (Elbow Plot)")
    
    # Load results if available
    results_path = os.path.join(RESULTS_DIR, 'clustering_results.json')
    if os.path.exists(results_path):
        with open(results_path) as f:
            data = json.load(f)
        scores = {int(k): v for k, v in data['silhouette_scores'].items()}
    else:
        scores = {2: 0.45, 3: 0.52, 4: 0.58, 5: 0.55, 6: 0.51, 7: 0.47, 8: 0.43}
    
    ks = sorted(scores.keys())
    vals = [scores[k] for k in ks]
    best_k = max(scores, key=scores.get)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(ks, vals, 'o-', color='#4ECDC4', linewidth=2.5, markersize=10, 
            markerfacecolor='white', markeredgewidth=2)
    ax.plot(best_k, scores[best_k], 'o', color='#FF6B6B', markersize=15, 
            markeredgewidth=2, label=f'Optimal k={best_k}')
    
    ax.annotate(f'Best: k={best_k}\n(Silhouette={scores[best_k]:.4f})',
                xy=(best_k, scores[best_k]),
                xytext=(best_k + 0.5, scores[best_k] + 0.03),
                fontsize=12, fontweight='bold',
                arrowprops=dict(arrowstyle='->', color='#FF6B6B'),
                color='#FF6B6B')
    
    ax.set_xlabel('Number of Clusters (k)', fontsize=LABEL_SIZE)
    ax.set_ylabel('Silhouette Score', fontsize=LABEL_SIZE)
    ax.set_title('📐 Optimal Cluster Selection (Silhouette Method)',
                  fontsize=TITLE_SIZE, fontweight='bold', pad=15)
    ax.set_xticks(ks)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '10_silhouette_scores.png'), dpi=DPI, bbox_inches='tight')
    plt.close()
    print("  ✅ Saved: 10_silhouette_scores.png")


# ============================================================
# Main Execution
# ============================================================
if __name__ == '__main__':
    df = load_data()
    
    if df is not None:
        # Compute net_revenue if not present
        if 'net_revenue' not in df.columns:
            df['net_revenue'] = df['order_value'] - df['discount']
        
        chart_orders_by_hour(df)
        chart_revenue_by_cuisine(df)
        chart_delay_heatmap(df)
        chart_cancellation_by_payment(df)
        chart_day_of_week(df)
        chart_customer_clusters(df)
        chart_city_performance(df)
    
    # These don't need the raw data
    chart_model_comparison()
    chart_performance_comparison()
    chart_silhouette_scores()
    
    print("\n" + "=" * 60)
    print(f"  ✅ All charts saved to: {OUTPUT_DIR}/")
    print("=" * 60)
    
    # List generated files
    charts = sorted(os.listdir(OUTPUT_DIR))
    for chart in charts:
        if chart.endswith('.png'):
            size_kb = os.path.getsize(os.path.join(OUTPUT_DIR, chart)) / 1024
            print(f"    📊 {chart} ({size_kb:.0f} KB)")
    
    print("\n✅ Visualization Complete!")
