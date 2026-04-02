const WORDS = [
  'Crunching',
  'Fetching',
  'Loading',
  'Thinking',
  'Computing',
  'Grinding',
  'Wrangling',
  'Cooking',
  'Digging',
  'Pulling',
];

export function randomLoadingWord(): string {
  return WORDS[Math.floor(Math.random() * WORDS.length)];
}
