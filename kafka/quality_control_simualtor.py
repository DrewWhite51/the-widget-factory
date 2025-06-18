import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from typing import Dict, Any
import logging
from schemas import QualityStatus, WidgetType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QualityControlSimulator:
    """Simulates quality control inspections for assembled widgets"""
    
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        
        self.consumer = KafkaConsumer(
            'widget-assembly',
            bootstrap_servers=bootstrap_servers,
            group_id='quality-control',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.inspectors = ["QC_INSPECTOR_A", "QC_INSPECTOR_B", "QC_INSPECTOR_C"]
        self.running = True
        
        # Quality standards by widget type
        self.quality_standards = {
            WidgetType.BASIC_WIDGET: {"min_performance": 75, "pass_rate": 0.95},
            WidgetType.PREMIUM_WIDGET: {"min_performance": 85, "pass_rate": 0.90},
            WidgetType.INDUSTRIAL_WIDGET: {"min_performance": 90, "pass_rate": 0.88},
            WidgetType.SMART_WIDGET: {"min_performance": 88, "pass_rate": 0.92}
        }
        
        self.stats = {
            "widgets_inspected": 0,
            "widgets_passed": 0,
            "widgets_failed": 0
        }
    
    def perform_quality_check(self, widget_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform quality inspection on a widget"""
        widget_type = WidgetType(widget_data['widget_type'])
        standards = self.quality_standards[widget_type]
        
        check_id = f"QC_{uuid.uuid4().hex[:8]}"
        inspector_id = random.choice(self.inspectors)
        
        # Simulate test results
        electrical_test = random.random() > 0.05  # 95% pass rate
        mechanical_test = random.random() > 0.03  # 97% pass rate
        visual_inspection = random.random() > 0.02  # 98% pass rate
        
        # Performance score based on widget type
        base_performance = standards["min_performance"]
        performance_score = max(0, min(100, 
            random.normalvariate(base_performance + 5, 8)
        ))
        
        # Determine overall pass/fail
        tests_passed = electrical_test and mechanical_test and visual_inspection
        performance_ok = performance_score >= standards["min_performance"]
        overall_pass = tests_passed and performance_ok and random.random() < standards["pass_rate"]
        
        # Generate defects if failed
        defects_found = []
        if not electrical_test:
            defects_found.append("electrical_failure")
        if not mechanical_test:
            defects_found.append("mechanical_defect")
        if not visual_inspection:
            defects_found.append("visual_defect")
        if not performance_ok:
            defects_found.append("performance_below_standard")
        
        quality_check_data = {
            "check_id": check_id,
            "widget_id": widget_data['widget_id'],
            "inspector_id": inspector_id,
            "checked_at": datetime.now().isoformat(),
            "status": QualityStatus.PASS.value if overall_pass else QualityStatus.FAIL.value,
            "test_results": {
                "electrical_test": electrical_test,
                "mechanical_test": mechanical_test,
                "visual_inspection": visual_inspection,
                "performance_score": round(performance_score, 2)
            },
            "defects_found": defects_found,
            "notes": f"Inspected by {inspector_id}. {'All tests passed.' if overall_pass else 'Failed quality standards.'}"
        }
        
        return quality_check_data
    
    def send_quality_check(self, quality_data: Dict[str, Any]):
        """Send quality check results to Kafka"""
        try:
            future = self.producer.send(
                'quality-control',
                value=quality_data,
                key=quality_data['widget_id']
            )
            result = future.get(timeout=10)
            
            status = quality_data['status']
            widget_id = quality_data['widget_id']
            logger.info(f"Quality check: {widget_id} - {status.upper()}")
            
            self.stats["widgets_inspected"] += 1
            if status == QualityStatus.PASS.value:
                self.stats["widgets_passed"] += 1
            else:
                self.stats["widgets_failed"] += 1
                logger.warning(f"Widget {widget_id} failed QC: {quality_data['defects_found']}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to send quality check: {e}")
            return False
    
    def listen_for_widgets(self):
        """Listen for completed widget assemblies and perform QC"""
        logger.info("Starting quality control listener...")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                widget_data = message.value
                
                # Simulate inspection time
                time.sleep(random.uniform(2, 5))
                
                # Perform quality check
                quality_data = self.perform_quality_check(widget_data)
                self.send_quality_check(quality_data)
                
                # Print stats every 10 widgets
                if self.stats["widgets_inspected"] % 10 == 0:
                    self.print_stats()
                
        except Exception as e:
            logger.error(f"Error in quality control listener: {e}")
        finally:
            self.consumer.close()
    
    def print_stats(self):
        """Print quality control statistics"""
        total = self.stats["widgets_inspected"]
        if total > 0:
            pass_rate = (self.stats["widgets_passed"] / total) * 100
            logger.info(f"=== QC STATS === Inspected: {total}, "
                       f"Passed: {self.stats['widgets_passed']}, "
                       f"Failed: {self.stats['widgets_failed']}, "
                       f"Pass Rate: {pass_rate:.1f}%")
    
    def close(self):
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    qc_simulator = QualityControlSimulator()
    
    try:
        qc_simulator.listen_for_widgets()
    except KeyboardInterrupt:
        logger.info("Quality control simulation stopped")
    finally:
        qc_simulator.close()