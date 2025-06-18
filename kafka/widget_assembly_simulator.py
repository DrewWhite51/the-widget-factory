import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from typing import Dict, Any, List
import logging
import threading
from schemas import WidgetType, ComponentType, WIDGET_RECIPES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WidgetAssemblySimulator:
    """Simulates widget assembly stations that consume components and produce widgets"""
    
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        
        self.consumer = KafkaConsumer(
            'component-production',
            bootstrap_servers=bootstrap_servers,
            group_id='assembly-stations',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Assembly stations
        self.assembly_stations = {
            "ASSEMBLY_STATION_A": [WidgetType.BASIC_WIDGET, WidgetType.PREMIUM_WIDGET],
            "ASSEMBLY_STATION_B": [WidgetType.INDUSTRIAL_WIDGET, WidgetType.SMART_WIDGET],
            "ASSEMBLY_STATION_C": [WidgetType.BASIC_WIDGET, WidgetType.SMART_WIDGET]
        }
        
        # Component inventory for each station
        self.component_inventory = {
            station: {component_type: [] for component_type in ComponentType}
            for station in self.assembly_stations
        }
        
        # Workers
        self.workers = ["WORKER_001", "WORKER_002", "WORKER_003", "WORKER_004", "WORKER_005"]
        
        # Production orders queue
        self.production_orders = []
        
        self.running = True
        self.stats = {
            "components_received": 0,
            "widgets_assembled": 0,
            "assembly_failures": 0
        }
    
    def add_component_to_inventory(self, component_data: Dict[str, Any]):
        """Add received component to station inventories"""
        component_type = ComponentType(component_data['component_type'])
        component_info = {
            'component_id': component_data['component_id'],
            'batch_id': component_data['batch_id'],
            'received_at': datetime.now().isoformat(),
            'quality_metrics': component_data['quality_metrics']
        }
        
        # Add to all stations (in reality, there would be routing logic)
        for station in self.assembly_stations:
            self.component_inventory[station][component_type].append(component_info)
        
        self.stats["components_received"] += 1
        logger.debug(f"Added component {component_data['component_id']} to inventory")
    
    def check_component_availability(self, station_id: str, widget_type: WidgetType) -> bool:
        """Check if station has all required components for widget type"""
        required_components = WIDGET_RECIPES[widget_type]
        
        for component_type in required_components:
            if len(self.component_inventory[station_id][component_type]) == 0:
                return False
        return True
    
    def consume_components_for_widget(self, station_id: str, widget_type: WidgetType) -> List[str]:
        """Consume required components from inventory and return component IDs"""
        required_components = WIDGET_RECIPES[widget_type]
        consumed_component_ids = []
        
        for component_type in required_components:
            if self.component_inventory[station_id][component_type]:
                component = self.component_inventory[station_id][component_type].pop(0)
                consumed_component_ids.append(component['component_id'])
        
        return consumed_component_ids
    
    def calculate_assembly_time(self, widget_type: WidgetType) -> float:
        """Calculate realistic assembly time based on widget complexity"""
        base_times = {
            WidgetType.BASIC_WIDGET: 300,      # 5 minutes
            WidgetType.PREMIUM_WIDGET: 450,    # 7.5 minutes
            WidgetType.INDUSTRIAL_WIDGET: 600, # 10 minutes
            WidgetType.SMART_WIDGET: 540       # 9 minutes
        }
        
        base_time = base_times[widget_type]
        # Add worker skill variability (±20%)
        actual_time = base_time * random.uniform(0.8, 1.2)
        return actual_time
    
    def assemble_widget(self, station_id: str, widget_type: WidgetType, worker_id: str) -> Dict[str, Any]:
        """Simulate widget assembly process"""
        
        if not self.check_component_availability(station_id, widget_type):
            logger.warning(f"Station {station_id} lacks components for {widget_type.value}")
            return None
        
        widget_id = f"WIDGET_{widget_type.value.upper()}_{uuid.uuid4().hex[:8]}"
        assembly_start = datetime.now()
        
        # Consume components
        component_ids = self.consume_components_for_widget(station_id, widget_type)
        
        # Calculate assembly time
        assembly_duration = self.calculate_assembly_time(widget_type)
        
        # Simulate assembly time (scaled down for demo)
        time.sleep(assembly_duration / 100)  # Scale down by 100x for demo
        
        assembly_end = datetime.now()
        
        # Generate batch ID for the widget
        batch_id = f"WIDGET_BATCH_{datetime.now().strftime('%Y%m%d_%H%M')}_{random.randint(100, 999)}"
        
        widget_data = {
            "widget_id": widget_id,
            "widget_type": widget_type.value,
            "assembly_station_id": station_id,
            "component_ids": component_ids,
            "assembly_started_at": assembly_start.isoformat(),
            "assembly_completed_at": assembly_end.isoformat(),
            "assembly_duration_seconds": assembly_duration,
            "worker_id": worker_id,
            "batch_id": batch_id
        }
        
        return widget_data
    
    def send_widget_assembled(self, widget_data: Dict[str, Any]):
        """Send widget assembly completion event to Kafka"""
        try:
            future = self.producer.send(
                'widget-assembly',
                value=widget_data,
                key=widget_data['widget_id']
            )
            result = future.get(timeout=10)
            logger.info(f"Widget assembled: {widget_data['widget_id']} "
                       f"({widget_data['widget_type']}) at {widget_data['assembly_station_id']}")
            self.stats["widgets_assembled"] += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send widget assembly event: {e}")
            self.stats["assembly_failures"] += 1
            return False
    
    def add_production_order(self, widget_type: WidgetType, quantity: int, priority: int = 1):
        """Add a production order to the queue"""
        order = {
            "order_id": f"ORDER_{uuid.uuid4().hex[:8]}",
            "widget_type": widget_type,
            "quantity": quantity,
            "priority": priority,
            "created_at": datetime.now().isoformat(),
            "completed": 0
        }
        self.production_orders.append(order)
        logger.info(f"Added production order: {quantity}x {widget_type.value}")
    
    def process_production_orders(self):
        """Process production orders based on component availability"""
        if not self.production_orders:
            return
        
        # Sort orders by priority (higher number = higher priority)
        self.production_orders.sort(key=lambda x: x['priority'], reverse=True)
        
        for order in self.production_orders[:]:
            if order['completed'] >= order['quantity']:
                self.production_orders.remove(order)
                logger.info(f"Order {order['order_id']} completed!")
                continue
            
            widget_type = order['widget_type']
            
            # Find a station that can build this widget type and has components
            available_stations = [
                station for station, capabilities in self.assembly_stations.items()
                if widget_type in capabilities and self.check_component_availability(station, widget_type)
            ]
            
            if available_stations:
                station_id = random.choice(available_stations)
                worker_id = random.choice(self.workers)
                
                widget_data = self.assemble_widget(station_id, widget_type, worker_id)
                if widget_data:
                    self.send_widget_assembled(widget_data)
                    order['completed'] += 1
    
    def listen_for_components(self):
        """Listen for component production events"""
        logger.info("Starting component listener...")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                component_data = message.value
                self.add_component_to_inventory(component_data)
                
                # Try to process any pending orders
                self.process_production_orders()
                
        except Exception as e:
            logger.error(f"Error in component listener: {e}")
        finally:
            self.consumer.close()
    
    def simulate_production_schedule(self, duration_minutes: int = 60):
        """Simulate a production schedule with varying demand"""
        logger.info(f"Starting {duration_minutes}-minute production simulation")
        
        # Start component listener in background
        listener_thread = threading.Thread(target=self.listen_for_components)
        listener_thread.daemon = True
        listener_thread.start()
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Add initial production orders
        self.add_production_order(WidgetType.BASIC_WIDGET, 10, priority=2)
        self.add_production_order(WidgetType.PREMIUM_WIDGET, 5, priority=3)
        self.add_production_order(WidgetType.SMART_WIDGET, 3, priority=1)
        
        try:
            while datetime.now() < end_time and self.running:
                # Randomly add new orders during the shift
                if random.random() < 0.1:  # 10% chance every iteration
                    widget_type = random.choice(list(WidgetType))
                    quantity = random.randint(1, 5)
                    priority = random.randint(1, 3)
                    self.add_production_order(widget_type, quantity, priority)
                
                # Process existing orders
                self.process_production_orders()
                
                # Print status every 5 minutes
                if int((datetime.now() - start_time).total_seconds()) % 300 == 0:
                    self.print_status()
                
                time.sleep(10)  # Check every 10 seconds
                
        except KeyboardInterrupt:
            logger.info("Production simulation interrupted")
        finally:
            self.running = False
    
    def print_status(self):
        """Print current production status"""
        logger.info("=== ASSEMBLY STATUS ===")
        logger.info(f"Components received: {self.stats['components_received']}")
        logger.info(f"Widgets assembled: {self.stats['widgets_assembled']}")
        logger.info(f"Assembly failures: {self.stats['assembly_failures']}")
        logger.info(f"Pending orders: {len(self.production_orders)}")
        
        # Print inventory summary
        for station, inventory in self.component_inventory.items():
            total_components = sum(len(components) for components in inventory.values())
            logger.info(f"{station}: {total_components} components in stock")
    
    def close(self):
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    simulator = WidgetAssemblySimulator()
    
    try:
        # Option 1: Run with component listener (requires component producer running)
        simulator.simulate_production_schedule(duration_minutes=30)
        
        # Option 2: Add manual production orders and process them
        # simulator.add_production_order(WidgetType.BASIC_WIDGET, 5)
        # simulator.add_production_order(WidgetType.PREMIUM_WIDGET, 3)
        # 
        # # Manually add some components to test assembly
        # for _ in range(20):
        #     fake_component = {
        #         'component_id': f'TEST_COMP_{uuid.uuid4().hex[:8]}',
        #         'component_type': random.choice(list(ComponentType)).value,
        #         'batch_id': 'TEST_BATCH',
        #         'quality_metrics': {'defect_count': 0}
        #     }
        #     simulator.add_component_to_inventory(fake_component)
        # 
        # simulator.process_production_orders()
        
    finally:
        simulator.close()