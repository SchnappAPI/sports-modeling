const WORDS = [
  'Moseying',
  'Gallivanting',
  'Waddling',
  'Skedaddling',
  'Slithering',
  'Frolicking',
  'Shimmying',
  'Lollygagging',
  'Puttering',
  'Schlepping',
  'Zigzagging',
  'Spelunking',
  'Osmosing',
  'Noodling',
  'Finagling',
  'Pontificating',
  'Discombobulating',
  'Flummoxing',
  'Razzle-dazzling',
  'Canoodling',
  'Recombobulating',
  'Reticulating',
  'Smooshing',
  'Wibbling',
  'Topsy-turvying',
  'Razzmatazzing',
];

export function randomLoadingWord(): string {
  return WORDS[Math.floor(Math.random() * WORDS.length)];
}
