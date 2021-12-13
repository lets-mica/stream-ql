package net.dreamlu.stream.ql;


import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public class DefaultStreamQLContext implements StreamQLContext {
    private final Function<String, Stream<Object>> supplier;
    private final List<Object> parameter = new ArrayList<>();
    private final Map<String, Object> namedParameter = new HashMap<>();

    private BiFunction<String, Stream<Object>, Stream<Object>> mapper = (s, stream) -> stream;

    public DefaultStreamQLContext(Function<String, ? extends Stream<?>> supplier) {
        this.supplier = (Function<String, Stream<Object>>) supplier;
    }

    @Override
    public Map<String, Object> getParameters() {
        return namedParameter;
    }

    @Override
    public StreamQLContext bind(Object value) {
        parameter.add(value);
        return this;
    }

    @Override
    public StreamQLContext bind(int index, Object value) {
        parameter.add(index, value);
        return this;
    }

    @Override
    public StreamQLContext bind(String name, Object value) {
        if (name != null && value != null) {
            namedParameter.put(name, value);
        }
        return this;
    }

    @Override
    public Stream<Object> getDataSource(String name) {
        name = getCleanStr(name);
        Stream<Object> stream = supplier.apply(name);
        return mapper.apply(name, stream);
    }

    @Override
    public Optional<Object> getParameter(int index) {
        if (parameter.size() <= (index)) {
            return Optional.empty();
        }
        return Optional.ofNullable(parameter.get(index));
    }

    @Override
    public Optional<Object> getParameter(String name) {
        return Optional.ofNullable(namedParameter.get(getCleanStr(name)));
    }

    @Override
    public StreamQLContext transfer(BiFunction<String, Stream<Object>, Stream<Object>> dataSourceMapper) {
        DefaultStreamQLContext context = new DefaultStreamQLContext(supplier);
        context.mapper = dataSourceMapper;
        return context;
    }

    private static String getCleanStr(String str) {
        if (str == null) {
            return null;
        }
        if (str.startsWith("\"") || str.startsWith("'")) {
            str = str.substring(1);
        }
        if (str.endsWith("\"") || str.endsWith("'")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }
}
