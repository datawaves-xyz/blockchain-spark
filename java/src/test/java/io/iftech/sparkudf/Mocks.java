package io.iftech.sparkudf;

import java.util.List;

public class Mocks {

    public static class FunctionField {

        public String name;
        public String type;
        public List<FunctionField> components;

        public FunctionField(String type) {
            this.type = type;
        }

        public FunctionField(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public FunctionField(String name, String type, List<FunctionField> components) {
            this(name, type);
            this.components = components;
        }
    }

    public static class ContractFunction {

        public String name = "test_function";
        public String type = "function";
        public List<FunctionField> inputs;
        public List<FunctionField> outputs;
    }

    public static class EventField {

        public String name;
        public String type;
        public boolean indexed;

        public EventField(String name, String type, boolean indexed) {
            this.name = name;
            this.type = type;
            this.indexed = indexed;
        }

        public EventField(String name, String type) {
            this(name, type, false);
        }
    }

    public static class ContractEvent {

        public String name = "test_event";
        public String type = "event";
        public List<EventField> inputs;

        public ContractEvent(String name) {
            this.name = name;
        }
    }
}
