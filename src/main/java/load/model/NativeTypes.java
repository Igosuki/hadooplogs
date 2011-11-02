package load.model;

/**
 * Global enum for types
 */
enum NativeTypes
{
	UDEF("undefined"), SHORT("short"), STRING("string"), 
	INTEGER("integer"),
	LONG("long"), DOUBLE("double"), BOOL("boolean");
	String name;
	NativeTypes(String name) {
		this.name = name;
	}
	String getName() {return this.name;};

}
