from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, schemaless_writer
from pypfopt import EfficientFrontier, risk_models, expected_returns, objective_functions
import io
import json
import pandas as pd
import numpy as np
import time


import warnings
warnings.filterwarnings("ignore")

class PortfolioOptimizer:
    def __init__(self):
        self.SHARPE_THRESHOLD = 1.0
        self.MAX_ITERATIONS = 5  
        self.GAMMA_INCREMENT = 0.001  
        self.current_gamma = 0.002
        
        # Load schemas
        with open('avro/PortfolioMetrics.avsc') as f:
            self.portfolio_metrics_schema = json.load(f)
        with open('avro/PortfolioStats.avsc') as f:
            self.portfolio_stats_schema = json.load(f)
        with open('avro/Weights.avsc') as f:
            self.portfolio_weights_schema = json.load(f)
            
        # Kafka setup
        conf_consumer = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'reAllocation-group',
            'auto.offset.reset': 'latest'
        }
        producer_conf = {'bootstrap.servers': 'localhost:9092'}
        
        self.consumer_metrics = Consumer(conf_consumer)
        self.consumer_stats = Consumer(conf_consumer)
        self.producer = Producer(producer_conf)
        
        self.consumer_metrics.subscribe(['portfMetrics'])
        self.consumer_stats.subscribe(['portfUpdatedStats'])
        
        self.latest_stats = None
        self.iteration_count = 0

    def iterative_optimization(self, portfolio_stats):
        
        mean_returns = pd.Series(portfolio_stats['meanReturns'])
        cov_matrix = pd.DataFrame(portfolio_stats['covarianceMatrix'])
        
        best_sharpe = -np.inf
        best_weights = None
        
        for i in range(self.MAX_ITERATIONS):
            try:
                ef = EfficientFrontier(mean_returns, cov_matrix)
                
                
                current_gamma = self.current_gamma + (i * self.GAMMA_INCREMENT)
                ef.add_objective(objective_functions.L2_reg, gamma=current_gamma)
                
                
                if i == 0:
                    weights = ef.max_sharpe()
                elif i == 1:
                    weights = ef.efficient_risk(target_volatility=0.15)
                elif i == 2:
                    weights = ef.efficient_return(target_return=mean_returns.mean() * 1.1)
                else:
                    
                    perturbed_returns = mean_returns * (1 + np.random.normal(0, 0.01, len(mean_returns)))
                    ef_perturbed = EfficientFrontier(perturbed_returns, cov_matrix)
                    ef_perturbed.add_objective(objective_functions.L2_reg, gamma=current_gamma)
                    weights = ef_perturbed.max_sharpe()
                    ef = ef_perturbed
                
                expected_return, annual_volatility, sharpe_ratio = ef.portfolio_performance(verbose=False)
                
                if sharpe_ratio > best_sharpe:
                    best_sharpe = sharpe_ratio
                    best_weights = dict(ef.clean_weights())
                
                
                
                if sharpe_ratio >= self.SHARPE_THRESHOLD:
                    
                    break
                    
            except Exception as e:
                print(f"Optimization iteration {i+1} failed: {e}")
                continue
        
        if best_weights is None:
            raise ValueError("All optimization attempts failed")
            
        print(f"Final Sharpe: {best_sharpe:.4f} after {min(i+1, self.MAX_ITERATIONS)} iterations")
        return best_weights, best_sharpe

    def reallocation(self, portfolio_stats):
        
        try:
            weights, final_sharpe = self.iterative_optimization(portfolio_stats)
            
            new_weight_msg = {
                'weights': weights,
                'timestamp': portfolio_stats['timestamp'],
                'portfolio_id': portfolio_stats['portfolio_id'],
                'expected_sharpe': final_sharpe  
            }
            return new_weight_msg
            
        except Exception as e:
            print(f"Reallocation failed: {e}")
            return None

    def produce_weights(self, weights):
        if weights is None:
            return
        
        latest_weights = self.get_latest_weights()
        
        if latest_weights and latest_weights.get('portfolio_id') == weights["portfolio_id"]:
            if latest_weights.get('weights') == weights["weights"]:
                return
        else:
            self.save_latest_weights(weights)

            
        bytes_writer = io.BytesIO()
        print(weights)
        schemaless_writer(bytes_writer, self.portfolio_weights_schema, weights)
        
        self.producer.produce('Weights', bytes_writer.getvalue(), 
                            weights["portfolio_id"].encode("utf-8"))
        bytes_writer.close()
        self.producer.flush()
    
    def get_latest_weights(self):
        try:
            with open('latestWeights.json', 'r') as f:
                cont = f.read().strip()
                if not cont:
                    return None
                return json.loads(cont)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
    
    def save_latest_weights(self, weights):
        with open('latestWeights.json', 'w') as f:
            json.dump(weights, f, indent=4)

    def run(self):
        try:
            while True:
                
                msg_stats = self.consumer_stats.poll(timeout=1.0)
                if msg_stats and not msg_stats.error():
                    bytes_reader = io.BytesIO(msg_stats.value())
                    self.latest_stats = schemaless_reader(bytes_reader, self.portfolio_stats_schema)
                    
                
                msg_metrics = self.consumer_metrics.poll(timeout=1.0)
                if msg_metrics is None:
                    continue
                if msg_metrics.error():
                    continue

                try:
                    bytes_reader = io.BytesIO(msg_metrics.value())
                    metrics = schemaless_reader(bytes_reader, self.portfolio_metrics_schema)

                    current_sharpe = metrics['sharpRatio']
                    print(f"Current Sharpe: {current_sharpe:.4f}")

                    
                    if (current_sharpe < self.SHARPE_THRESHOLD and self.latest_stats ):

                        new_weights = self.reallocation(self.latest_stats)
                        if new_weights:
                            self.produce_weights(new_weights)
                           
   
                except Exception as e:
                    print(f"Failed processing: {e}")
                    

        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            self.consumer_metrics.close()
            self.consumer_stats.close()

if __name__ == "__main__":
    optimizer = PortfolioOptimizer()
    optimizer.run()