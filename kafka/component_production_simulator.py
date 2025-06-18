import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from typing import Dict, Any
import logging
from schemas import ComponentType, WIDGET_RECIPES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComponentProductionSimulator:
    """Simulates component production machines in the widget factory"""
    
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        
        # Factory configuration
        self.machines = {
            "FRAME_MACHINE_01": ComponentType.METAL_FRAME,
            "FRAME_MACHINE_02": ComponentType.METAL_FRAME,
            "PLASTIC_MOLD_01": ComponentType.PLASTIC_HOUSING,
            "PLASTIC_MOLD_02": ComponentType.PLASTIC_HOUSING,
            "PCB_ASSEMBLER_01": ComponentType.CIRCUIT_BOARD,
            "PCB_ASSEMBLER_02": ComponentType.CIRCUIT_BOARD,
            "BATTERY_PACK_01": ComponentType.BATTERY,
            "DISPLAY_ASSEMBLY_01": ComponentType.DISPLAY_SCREEN,
            "SENSOR_CALIB_01": ComponentType.SENSOR,
            "MOTOR_ASSEMBLY_01": ComponentType.MOTOR
        }
        
        self.production_lines = ["LINE_A", "LINE_B", "LINE_C"]
        self.current_batch = self._generate_batch_id()
        self.components_in_batch = 0
        self.max_batch_size = 50
        
    def _generate_batch_id(self) -> str:
        """Generate a new batch ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        return f"BATCH_{timestamp}_{random.randint(1000, 9999)}"
    
    def _generate_component_metrics(self, component_type: ComponentType) -> Dict[str, Any]:
        """Generate realistic quality metrics for different component types"""
        base_metrics = {
            "temperature_celsius": round(random.normalvariate(22, 2), 1),
            "defect_count": random.choices([0, 1, 2, 3], weights=[80, 15, 4, 1])[0]
        }
        
        # Component-specific metrics
        if component_type == ComponentType.METAL_FRAME:
            base_metrics.update({
                "weight_grams": round(random.normalvariate(150, 5), 2),
                "dimensions_mm": {
                    "length": round(random.normalvariate(100, 1), 1),
                    "width": round(random.normalvariate(50, 0.5), 1),
                    "height": round(random.normalvariate(25, 0.3), 1)
                }
            })
        elif component_type == ComponentType.PLASTIC_HOUSING:
            base_metrics.update({
                "weight_grams": round(random.normalvariate(75, 3), 2),
                "dimensions_mm": {
                    "length": round(random.normalvariate(98, 0.5), 1),
                    "width": round(random.normalvariate(48, 0.3), 1),
                    "height": round(random.normalvariate(23, 0.2), 1)
                }
            })
        elif component_type == ComponentType.CIRCUIT_BOARD:
            base_metrics.update({
                "weight_grams": round(random.normalvariate(25, 2), 2),
                "dimensions_mm": {
                    "length": round(random.normalvariate(80, 0.5), 1),
                    "width": round(random.normalvariate(40, 0.3), 1),
                    "height": round(random.normalvariate(2, 0.1), 1)
                }
            })
        elif component_type == ComponentType.BATTERY:
            base_metrics.update({
                "weight_grams": round(random.normalvariate(45, 2), 2),
                "dimensions_mm": {
                    "length": round(random.normalvariate(60, 0.5), 1),
                    "width": round(random.normalvariate(30, 0.3), 1),
                    "height": round(random.normalvariate(8, 0.2), 1)
                }
            })
        else:
            # Default for other components
            base_metrics.update({
                "weight_grams": round(random.normalvariate(30, 5), 2),
                "dimensions_mm": {
                    "length": round(random.normalvariate(40, 2), 1),
                    "width": round(random.normalvariate(30, 1), 1),
                    "height": round(random.normalvariate(10, 1), 1)
                }
            })
        
        return base_metrics
    
    def produce_component(self, machine_id: str) -> Dict[str, Any]:
        """Simulate production of a single component"""
        component_type = self.machines[machine_id]
        component_id = f"COMP_{component_type.value.upper()}_{uuid.uuid4().hex[:8]}"
        
        # Check if we need a new batch
        if self.components_in_batch >= self.max_batch_size:
            self.current_batch = self._generate_batch_id()
            self.components_in_batch = 0
            logger.info(f"Starting new batch: {self.current_batch}")
        
        component_data = {
            "component_id": component_id,
            "component_type": component_type.value,
            "machine_id": machine_id,
            "batch_id": self.current_batch,
            "produced_at": datetime.now().isoformat(),
            "quality_metrics": self._generate_component_metrics(component_type),
            "production_line": random.choice(self.production_lines)
        }
        
        self.components_in_batch += 1
        return component_data
    
    def send_component_produced(self, component_data: Dict[str, Any]):
        """Send component production event to Kafka"""
        try:
            future = self.producer.send(
                'component-production',
                value=component_data,
                key=component_data['component_id']
            )
            result = future.get(timeout=10)
            logger.info(f"Component produced: {component_data['component_id']} "
                       f"({component_data['component_type']}) on {component_data['machine_id']}")
            return True
        except Exception as e:
            logger.error(f"Failed to send component production event: {e}")
            return False
    
    def simulate_production_shift(self, duration_minutes: int = 60):
        """Simulate a production shift with realistic timing"""
        logger.info(f"Starting {duration_minutes}-minute production shift simulation")
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Different machines have different cycle times (in seconds)
        cycle_times = {
            "FRAME_MACHINE_01": random.randint(45, 75),
            "FRAME_MACHINE_02": random.randint(45, 75),
            "PLASTIC_MOLD_01": random.randint(90, 120),
            "PLASTIC_MOLD_02": random.randint(90, 120),
            "PCB_ASSEMBLER_01": random.randint(180, 240),
            "PCB_ASSEMBLER_02": random.randint(180, 240),
            "BATTERY_PACK_01": random.randint(60, 90),
            "DISPLAY_ASSEMBLY_01": random.randint(120, 180),
            "SENSOR_CALIB_01": random.randint(300, 420),
            "MOTOR_ASSEMBLY_01": random.randint(240, 300)
        }
        
        machine_next_production = {machine: datetime.now() for machine in self.machines}
        
        try:
            while datetime.now() < end_time:
                current_time = datetime.now()
                
                # Check each machine to see if it should produce
                for machine_id in self.machines:
                    if current_time >= machine_next_production[machine_id]:
                        # Simulate occasional machine downtime
                        if random.random() < 0.02:  # 2% chance of brief downtime
                            logger.warning(f"Machine {machine_id} experiencing brief downtime")
                            machine_next_production[machine_id] = current_time + timedelta(
                                seconds=random.randint(300, 900)  # 5-15 minute downtime
                            )
                            continue
                        
                        # Produce component
                        component_data = self.produce_component(machine_id)
                        self.send_component_produced(component_data)
                        
                        # Schedule next production
                        base_cycle_time = cycle_times[machine_id]
                        # Add some variability (±20%)
                        actual_cycle_time = base_cycle_time * random.uniform(0.8, 1.2)
                        machine_next_production[machine_id] = current_time + timedelta(
                            seconds=actual_cycle_time
                        )
                
                # Check every 5 seconds
                time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Production shift interrupted by user")
        
        logger.info(f"Production shift completed. Total batches: {self.current_batch}")
    
    def simulate_component_demand(self, widget_demand: Dict[str, int]):
        """Simulate component production based on widget demand"""
        logger.info("Calculating component demand based on widget orders...")
        
        component_demand = {}
        for widget_type, quantity in widget_demand.items():
            recipe = WIDGET_RECIPES.get(widget_type)
            if recipe:
                for component_type in recipe:
                    component_demand[component_type] = component_demand.get(component_type, 0) + quantity
        
        logger.info(f"Component demand: {component_demand}")
        
        # Produce components to meet demand
        for component_type, quantity in component_demand.items():
            # Find machines that can produce this component
            capable_machines = [machine_id for machine_id, machine_component_type 
                              in self.machines.items() if machine_component_type == component_type]
            
            components_per_machine = quantity // len(capable_machines)
            remaining = quantity % len(capable_machines)
            
            for i, machine_id in enumerate(capable_machines):
                machine_quantity = components_per_machine + (1 if i < remaining else 0)
                
                for _ in range(machine_quantity):
                    component_data = self.produce_component(machine_id)
                    self.send_component_produced(component_data)
                    time.sleep(random.uniform(0.1, 0.5))  # Brief delay between components
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    simulator = ComponentProductionSimulator()
    
    try:
        # Example: Simulate a 30-minute production shift
        simulator.simulate_production_shift(duration_minutes=30)
        
        # Or simulate production based on specific demand
        # widget_demand = {
        #     "basic_widget": 20,
        #     "premium_widget": 15,
        #     "smart_widget": 10
        # }
        # simulator.simulate_component_demand(widget_demand)
        
    finally:
        simulator.close()