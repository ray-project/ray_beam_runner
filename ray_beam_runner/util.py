import ray

from apache_beam.pipeline import PipelineVisitor

class PipelinePrinter(PipelineVisitor):
    def visit_value(self, value, producer_node):
        print(f"visit_value(value, {producer_node.full_label})")

    def visit_transform(self, transform_node):
        print(f"visit_transform({type(transform_node.transform)})")

    def enter_composite_transform(self, transform_node):
        print(f"enter_composite_transform({transform_node.full_label})")

    def leave_composite_transform(self, transform_node):
        print(f"leave_composite_transform({transform_node.full_label})")
