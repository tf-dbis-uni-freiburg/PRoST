public class Main {
	public static void main(final String[] args) throws Exception {
		String filename = args[0];

		Benchmark benchmark = new Benchmark();

		System.out.println("Loading " + filename);
		benchmark.loadData(filename);
		benchmark.saveData(filename);
	}
}
