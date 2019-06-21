public class Main {
	public static void main(final String[] args) {
		final String filename = args[0];

		final Benchmark benchmark = new Benchmark();

		System.out.println("Loading " + filename);
		benchmark.loadData(filename);
		benchmark.saveData(filename);
		System.out.println("Saved file: " + filename + "_filtered.csv");
	}
}
