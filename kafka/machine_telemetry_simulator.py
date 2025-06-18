import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any
import logging
import threading
from schemas import MachineStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MachineTelemetrySimulator:
    """Simulates IoT telemetry data from factory machines"""
    
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        
        # All machines in the factory
        self.machines = {
            # Component production machines
            "FRAME_MACHINE_01": {"type": "metal_forming", "building": "A", "floor": 1, "line": "LINE_A"},
            "FRAME_MACHINE_02": {"type": "metal_forming", "building": "A", "floor": 1, "line": "LINE_B"},
            "PLASTIC_MOLD_01": {"type": "injection_molding", "building": "A", "floor": 2, "line": "LINE_A"},
            "PLASTIC_MOLD_02": {"type": "injection_molding", "building": "A", "floor": 2, "line": "LINE_B"},
            "PCB_ASSEMBLER_01": {"type": "electronics_assembly", "building": "B", "floor": 1, "line": "LINE_A"},
            "PCB_ASSEMBLER_02": {"type": "electronics_assembly", "building": "B", "floor": 1, "line": "LINE_B"},
            "BATTERY_PACK_01": {"type": "battery_assembly", "building": "B", "floor": 2, "line": "LINE_A"},
            "DISPLAY_ASSEMBLY_01": {"type": "display_assembly", "building": "B", "floor": 2, "line": "LINE_B"},
            "SENSOR_CALIB_01": {"type": "calibration", "building": "C", "floor": 1, "line": "LINE_A"},
            "MOTOR_ASSEMBLY_01": {"type": "motor_assembly", "building": "C", "floor": 1, "line": "LINE_B"},
            
            # Assembly stations
            "ASSEMBLY_STATION_A": {"type": "widget_assembly", "building": "D", "floor": 1, "line": "LINE_A"},
            "ASSEMBLY_STATION_B": {"type": "widget_assembly", "building": "D", "floor": 1, "line": "LINE_B"},
            "ASSEMBLY_STATION_C": {"type": "widget_assembly", "building": "D", "floor": 1, "line": "LINE_C"},
        }
        
        # Machine states
        self.machine_states = {
            machine_id: {
                "status": MachineStatus.IDLE,
                "cycle_count": 0,
                "last_maintenance": datetime.now(),
                "base_temperature": random.uniform(20, 25),
                "base_vibration": random.uniform(0.1, 0.5),
                "base_power": random.uniform(500, 2000)
            }
            for machine_id in self.machines
        }
        
        self.running = True
    
    def generate_telemetry(self, machine_id: str) -> Dict[str, Any]:
        """Generate realistic telemetry data for a machine"""
        machine_info = self.machines[machine_id]
        machine_state = self.machine_states[machine_id]
        
        # Simulate status changes
        if random.random() < 0.01:  # 1% chance of status change
            new_status = random.choice([
                MachineStatus.RUNNING, MachineStatus.IDLE, 
                MachineStatus.MAINTENANCE, MachineStatus.ERROR
            ])
            machine_state["status"] = new_status
        
        # Generate metrics based on status
        status = machine_state["status"]
        
        if status == MachineStatus.RUNNING:
            temperature = machine_state["base_temperature"] + random.normalvariate(15, 3)
            vibration = machine_state["base_vibration"] + random.normalvariate(0.3, 0.1)
            power = machine_state["base_power"] + random.normalvariate(300, 50)
            efficiency = random.normalvariate(85, 5)
            machine_state["cycle_count"] += random.randint(0, 2)
            
        elif status == MachineStatus.IDLE:
            temperature = machine_state["base_temperature"] + random.normalvariate(2, 1)
            vibration = machine_state["base_vibration"] + random.normalvariate(0.05, 0.02)
            power = machine_state["base_power"] * 0.1 + random.normalvariate(0, 10)
            efficiency = 0
            
        elif status == MachineStatus.ERROR:
            temperature = machine_state["base_temperature"] + random.normalvariate(25, 5)
            vibration = machine_state["base_vibration"] + random.normalvariate(1.5, 0.3)
            power = machine_state["base_power"] * 0.05 + random.normalvariate(0, 5)
            efficiency = 0
            
        elif status == MachineStatus.MAINTENANCE:
            temperature = machine_state["base_temperature"] + random.normalvariate(1, 0.5)
            vibration = machine_state["base_vibration"] + random.normalvariate(0.02, 0.01)
            power = machine_state["base_power"] * 0.02 + random.normalvariate(0, 2)
            efficiency = 0
            
        else:  # SHUTDOWN
            temperature = machine_state["base_temperature"]
            vibration = 0
            power = 0
            efficiency = 0
        
        # Ensure reasonable bounds
        temperature = max(0, min(100, temperature))
        vibration = max(0, min(10, vibration))
        power = max(0, power)
        efficiency = max(0, min(100, efficiency))
        
        telemetry_data = {
            "machine_id": machine_id,
            "machine_type": machine_info["type"],
            "timestamp": datetime.now().isoformat(),
            "status": status.value,
            "metrics": {
                "temperature_celsius": round(temperature, 2),
                "vibration_level": round(vibration, 3),
                "power_consumption_watts": round(power, 2),
                "cycle_count": machine_state["cycle_count"],
                "efficiency_percent": round(efficiency, 2)
            },
            "location": {
                "building": machine_info["building"],
                "floor": machine_info["floor"],
                "line": machine_info["line"]
            }
        }
        
        return telemetry_data
    
    def send_telemetry(self, telemetry_data: Dict[str, Any]):
        """Send telemetry data to Kafka"""
        try:
            future = self.producer.send(
                'machine-telemetry',
                value=telemetry_data,
                key=telemetry_data['machine_id']
            )
            future.get(timeout=5)
            return True
        except Exception as e:
            logger.error(f"Failed to send telemetry for {telemetry_data['machine_id']}: {e}")
            return False
    
    def simulate_machine_telemetry(self, machine_id: str, interval_seconds: int = 30):
        """Simulate continuous telemetry for a single machine"""
        logger.info(f"Starting telemetry simulation for {machine_id}")
        
        while self.running:
            telemetry_data = self.generate_telemetry(machine_id)
            self.send_telemetry(telemetry_data)
            
            # Log status changes and errors
            status = telemetry_data['status']
            if status == 'error':
                logger.warning(f"Machine {machine_id} in ERROR state!")
            elif status == 'maintenance':
                logger.info(f"Machine {machine_id} under maintenance")
            
            time.sleep(interval_seconds)
    
    def simulate_all_machines(self, interval_seconds: int = 30):
        """Simulate telemetry for all machines using threads"""
        logger.info(f"Starting telemetry simulation for {len(self.machines)} machines")
        
        threads = []
        for machine_id in self.machines:
            thread = threading.Thread(
                target=self.simulate_machine_telemetry,
                args=(machine_id, interval_seconds),
                daemon=True
            )
            threads.append(thread)
            thread.start()
            time.sleep(1)  # Stagger thread starts
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(10)
                # Print status summary
                self.print_machine_status()
        except KeyboardInterrupt:
            logger.info("Telemetry simulation interrupted")
        finally:
            self.running = False
            for thread in threads:
                thread.join(timeout=5)
    
    def print_machine_status(self):
        """Print summary of all machine statuses"""
        status_count = {}
        for machine_id, state in self.machine_states.items():
            status = state["status"].value
            status_count[status] = status_count.get(status, 0) + 1
        
        logger.info(f"Machine Status Summary: {status_count}")
    
    def simulate_anomaly(self, machine_id: str):
        """Inject an anomaly into a specific machine"""
        if machine_id in self.machine_states:
            self.machine_states[machine_id]["status"] = MachineStatus.ERROR
            logger.warning(f"Anomaly injected into {machine_id}")
        else:
            logger.error(f"Machine {machine_id} not found")
    
    def close(self):
        self.running = False
        self.producer.close()

if __name__ == "__main__":
    telemetry_simulator = MachineTelemetrySimulator()
    
    try:
        # Option 1: Simulate all machines
        telemetry_simulator.simulate_all_machines(interval_seconds=15)
        
        # Option 2: Simulate specific machines
        # telemetry_simulator.simulate_machine_telemetry("FRAME_MACHINE_01", interval_seconds=10)
        
    finally:
        telemetry_simulator.close()